#pragma once

#include <ext/shared_ptr_helper.h>
#include <atomic>
#include <pcg_random.hpp>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreePartsMover.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Storages/MergeTree/DataPartsExchange.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/LeaderElection.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/PartLog.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <Processors/Pipe.h>


namespace DB
{

/** The engine that uses the merge tree (see MergeTreeData) and replicated through ZooKeeper.
  *
  * ZooKeeper is used for the following things:
  * - the structure of the table (/metadata, /columns)
  * - action log with data (/log/log-...,/replicas/replica_name/queue/queue-...);
  * - a replica list (/replicas), and replica activity tag (/replicas/replica_name/is_active), replica addresses (/replicas/replica_name/host);
  * - select the leader replica (/leader_election) - this is the replica that assigns the merge;
  * - a set of parts of data on each replica (/replicas/replica_name/parts);
  * - list of the last N blocks of data with checksum, for deduplication (/blocks);
  * - the list of incremental block numbers (/block_numbers) that we are about to insert,
  *   to ensure the linear order of data insertion and data merge only on the intervals in this sequence;
  * - coordinates writes with quorum (/quorum).
  * - Storage of mutation entries (ALTER DELETE, ALTER UPDATE etc.) to execute (/mutations).
  *   See comments in StorageReplicatedMergeTree::mutate() for details.
  */

/** The replicated tables have a common log (/log/log-...).
  * Log - a sequence of entries (LogEntry) about what to do.
  * Each entry is one of:
  * - normal data insertion (GET),
  * - merge (MERGE),
  * - delete the partition (DROP).
  *
  * Each replica copies (queueUpdatingTask, pullLogsToQueue) entries from the log to its queue (/replicas/replica_name/queue/queue-...)
  *  and then executes them (queueTask).
  * Despite the name of the "queue", execution can be reordered, if necessary (shouldExecuteLogEntry, executeLogEntry).
  * In addition, the records in the queue can be generated independently (not from the log), in the following cases:
  * - when creating a new replica, actions are put on GET from other replicas (createReplica);
  * - if the part is corrupt (removePartAndEnqueueFetch) or absent during the check (at start - checkParts, while running - searchForMissingPart),
  *   actions are put on GET from other replicas;
  *
  * The replica to which INSERT was made in the queue will also have an entry of the GET of this data.
  * Such an entry is considered to be executed as soon as the queue handler sees it.
  *
  * The log entry has a creation time. This time is generated by the clock of server that created entry
  * - the one on which the corresponding INSERT or ALTER query came.
  *
  * For the entries in the queue that the replica made for itself,
  * as the time will take the time of creation the appropriate part on any of the replicas.
  */

class StorageReplicatedMergeTree final : public ext::shared_ptr_helper<StorageReplicatedMergeTree>, public MergeTreeData
{
    friend struct ext::shared_ptr_helper<StorageReplicatedMergeTree>;
public:
    void startup() override;
    void shutdown() override;
    ~StorageReplicatedMergeTree() override;

    std::string getName() const override { return "Replicated" + merging_params.getModeName() + "MergeTree"; }

    bool supportsParallelInsert() const override { return true; }
    bool supportsReplication() const override { return true; }
    bool supportsDeduplication() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    std::optional<UInt64> totalRows() const override;
    std::optional<UInt64> totalBytes() const override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Context & query_context) override;

    void alter(const AlterCommands & params, const Context & query_context, TableLockHolder & table_lock_holder) override;

    Pipe alterPartition(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        const Context & query_context) override;

    void mutate(const MutationCommands & commands, const Context & context) override;
    void waitMutation(const String & znode_name, size_t mutations_sync) const;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    CancellationCode killMutation(const String & mutation_id) override;

    /** Removes a replica from ZooKeeper. If there are no other replicas, it deletes the entire table from ZooKeeper.
      */
    void drop() override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override;

    bool supportsIndexForIn() const override { return true; }

    void checkTableCanBeDropped() const override;

    ActionLock getActionLock(StorageActionBlockType action_type) override;

    /// Wait when replication queue size becomes less or equal than queue_size
    /// If timeout is exceeded returns false
    bool waitForShrinkingQueueSize(size_t queue_size = 0, UInt64 max_wait_milliseconds = 0);

    /** For the system table replicas. */
    struct Status
    {
        bool is_leader;
        bool can_become_leader;
        bool is_readonly;
        bool is_session_expired;
        ReplicatedMergeTreeQueue::Status queue;
        UInt32 parts_to_check;
        String zookeeper_path;
        String replica_name;
        String replica_path;
        Int32 columns_version;
        UInt64 log_max_index;
        UInt64 log_pointer;
        UInt64 absolute_delay;
        UInt8 total_replicas;
        UInt8 active_replicas;
        /// If the error has happened fetching the info from ZooKeeper, this field will be set.
        String zookeeper_exception;
    };

    /// Get the status of the table. If with_zk_fields = false - do not fill in the fields that require queries to ZK.
    void getStatus(Status & res, bool with_zk_fields = true);

    using LogEntriesData = std::vector<ReplicatedMergeTreeLogEntryData>;
    void getQueue(LogEntriesData & res, String & replica_name);

    /// Get replica delay relative to current time.
    time_t getAbsoluteDelay() const;

    /// If the absolute delay is greater than min_relative_delay_to_measure,
    /// will also calculate the difference from the unprocessed time of the best replica.
    /// NOTE: Will communicate to ZooKeeper to calculate relative delay.
    void getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay);

    /// Add a part to the queue of parts whose data you want to check in the background thread.
    void enqueuePartForCheck(const String & part_name, time_t delay_to_check_seconds = 0)
    {
        part_check_thread.enqueuePart(part_name, delay_to_check_seconds);
    }

    CheckResults checkData(const ASTPtr & query, const Context & context) override;

    /// Checks ability to use granularity
    bool canUseAdaptiveGranularity() const override;

    int getMetadataVersion() const { return metadata_version; }

    /** Remove a specific replica from zookeeper.
     */
    static void dropReplica(zkutil::ZooKeeperPtr zookeeper, const String & zookeeper_path, const String & replica, Poco::Logger * logger);

private:

    /// Get a sequential consistent view of current parts.
    ReplicatedMergeTreeQuorumAddedParts::PartitionIdToMaxBlock getMaxAddedBlocks() const;

    /// Delete old parts from disk and from ZooKeeper.
    void clearOldPartsAndRemoveFromZK();

    friend class ReplicatedMergeTreeBlockOutputStream;
    friend class ReplicatedMergeTreePartCheckThread;
    friend class ReplicatedMergeTreeCleanupThread;
    friend class ReplicatedMergeTreeAlterThread;
    friend class ReplicatedMergeTreeRestartingThread;
    friend struct ReplicatedMergeTreeLogEntry;
    friend class ScopedPartitionMergeLock;
    friend class ReplicatedMergeTreeQueue;
    friend class MergeTreeData;

    using LogEntry = ReplicatedMergeTreeLogEntry;
    using LogEntryPtr = LogEntry::Ptr;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

    /// If true, the table is offline and can not be written to it.
    std::atomic_bool is_readonly {false};
    /// If false - ZooKeeper is available, but there is no table metadata. It's safe to drop table in this case.
    bool has_metadata_in_zookeeper = true;

    String zookeeper_path;
    String replica_name;
    String replica_path;

    /** /replicas/me/is_active.
      */
    zkutil::EphemeralNodeHolderPtr replica_is_active_node;

    /** Is this replica "leading". The leader replica selects the parts to merge.
      * It can be false only when old ClickHouse versions are working on the same cluster, because now we allow multiple leaders.
      */
    std::atomic<bool> is_leader {false};
    zkutil::LeaderElectionPtr leader_election;

    InterserverIOEndpointPtr data_parts_exchange_endpoint;

    MergeTreeDataSelectExecutor reader;
    MergeTreeDataWriter writer;
    MergeTreeDataMergerMutator merger_mutator;

    /** The queue of what needs to be done on this replica to catch up with everyone. It is taken from ZooKeeper (/replicas/me/queue/).
     * In ZK entries in chronological order. Here it is not necessary.
     */
    ReplicatedMergeTreeQueue queue;
    std::atomic<time_t> last_queue_update_start_time{0};
    std::atomic<time_t> last_queue_update_finish_time{0};

    DataPartsExchange::Fetcher fetcher;


    /// When activated, replica is initialized and startup() method could exit
    Poco::Event startup_event;

    /// Do I need to complete background threads (except restarting_thread)?
    std::atomic<bool> partial_shutdown_called {false};

    /// Event that is signalled (and is reset) by the restarting_thread when the ZooKeeper session expires.
    Poco::Event partial_shutdown_event {false};     /// Poco::Event::EVENT_MANUALRESET

    /// Limiting parallel fetches per one table
    std::atomic_uint current_table_fetches {0};

    int metadata_version = 0;
    /// Threads.

    /// A task that keeps track of the updates in the logs of all replicas and loads them into the queue.
    bool queue_update_in_progress = false;
    BackgroundSchedulePool::TaskHolder queue_updating_task;

    BackgroundSchedulePool::TaskHolder mutations_updating_task;

    /// A task that performs actions from the queue.
    BackgroundProcessingPool::TaskHandle queue_task_handle;

    /// A task which move parts to another disks/volumes
    /// Transparent for replication.
    BackgroundProcessingPool::TaskHandle move_parts_task_handle;

    /// A task that selects parts to merge.
    BackgroundSchedulePool::TaskHolder merge_selecting_task;
    /// It is acquired for each iteration of the selection of parts to merge or each OPTIMIZE query.
    std::mutex merge_selecting_mutex;

    /// A task that marks finished mutations as done.
    BackgroundSchedulePool::TaskHolder mutations_finalizing_task;

    /// A thread that removes old parts, log entries, and blocks.
    ReplicatedMergeTreeCleanupThread cleanup_thread;

    /// A thread that checks the data of the parts, as well as the queue of the parts to be checked.
    ReplicatedMergeTreePartCheckThread part_check_thread;

    /// A thread that processes reconnection to ZooKeeper when the session expires.
    ReplicatedMergeTreeRestartingThread restarting_thread;

    /// True if replica was created for existing table with fixed granularity
    bool other_replicas_fixed_granularity = false;

    template <class Func>
    void foreachCommittedParts(const Func & func) const;

    /** Creates the minimum set of nodes in ZooKeeper and create first replica.
      * Returns true if was created, false if exists.
      */
    bool createTableIfNotExists(const StorageMetadataPtr & metadata_snapshot);

    /** Creates a replica in ZooKeeper and adds to the queue all that it takes to catch up with the rest of the replicas.
      */
    void createReplica(const StorageMetadataPtr & metadata_snapshot);

    /** Create nodes in the ZK, which must always be, but which might not exist when older versions of the server are running.
      */
    void createNewZooKeeperNodes();

    void checkTableStructure(const String & zookeeper_prefix, const StorageMetadataPtr & metadata_snapshot);

    /// A part of ALTER: apply metadata changes only (data parts are altered separately).
    /// Must be called under IStorage::lockForAlter() lock.
    void setTableStructure(ColumnsDescription new_columns, const ReplicatedMergeTreeTableMetadata::Diff & metadata_diff);

    /** Check that the set of parts corresponds to that in ZK (/replicas/me/parts/).
      * If any parts described in ZK are not locally, throw an exception.
      * If any local parts are not mentioned in ZK, remove them.
      *  But if there are too many, throw an exception just in case - it's probably a configuration error.
      */
    void checkParts(bool skip_sanity_checks);

    /** Check that the part's checksum is the same as the checksum of the same part on some other replica.
      * If no one has such a part, nothing checks.
      * Not very reliable: if two replicas add a part almost at the same time, no checks will occur.
      * Adds actions to `ops` that add data about the part into ZooKeeper.
      * Call under lockForShare.
      */
    void checkPartChecksumsAndAddCommitOps(const zkutil::ZooKeeperPtr & zookeeper, const DataPartPtr & part,
                                           Coordination::Requests & ops, String part_name = "", NameSet * absent_replicas_paths = nullptr);

    String getChecksumsForZooKeeper(const MergeTreeDataPartChecksums & checksums) const;

    /// Accepts a PreComitted part, atomically checks its checksums with ones on other replicas and commit the part
    DataPartsVector checkPartChecksumsAndCommit(Transaction & transaction,
                                                               const DataPartPtr & part);

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;

    void getCommitPartOps(Coordination::Requests & ops, MutableDataPartPtr & part, const String & block_id_path = "") const;

    /// Adds actions to `ops` that remove a part from ZooKeeper.
    /// Set has_children to true for "old-style" parts (those with /columns and /checksums child znodes).
    void removePartFromZooKeeper(const String & part_name, Coordination::Requests & ops, bool has_children);

    /// Quickly removes big set of parts from ZooKeeper (using async multi queries)
    void removePartsFromZooKeeper(zkutil::ZooKeeperPtr & zookeeper, const Strings & part_names,
                                  NameSet * parts_should_be_retried = nullptr);

    bool tryRemovePartsFromZooKeeperWithRetries(const Strings & part_names, size_t max_retries = 5);
    bool tryRemovePartsFromZooKeeperWithRetries(DataPartsVector & parts, size_t max_retries = 5);

    /// Removes a part from ZooKeeper and adds a task to the queue to download it. It is supposed to do this with broken parts.
    void removePartAndEnqueueFetch(const String & part_name);

    /// Running jobs from the queue.

    /** Execute the action from the queue. Throws an exception if something is wrong.
      * Returns whether or not it succeeds. If it did not work, write it to the end of the queue.
      */
    bool executeLogEntry(LogEntry & entry);


    void executeDropRange(const LogEntry & entry);

    /// Do the merge or recommend to make the fetch instead of the merge
    bool tryExecuteMerge(const LogEntry & entry);

    /// Execute alter of table metadata. Set replica/metadata and replica/columns
    /// nodes in zookeeper and also changes in memory metadata.
    /// New metadata and columns values stored in entry.
    bool executeMetadataAlter(const LogEntry & entry);

    /// Execute MUTATE_PART entry. Part name and mutation commands
    /// stored in entry. This function relies on MergerMutator class.
    bool tryExecutePartMutation(const LogEntry & entry);


    /// Fetch part from other replica (inserted or merged/mutated)
    /// NOTE: Attention! First of all tries to find covering part on other replica
    /// and set it into entry.actual_new_part_name. After that tries to fetch this new covering part.
    /// If fetch was not successful, clears entry.actual_new_part_name.
    bool executeFetch(LogEntry & entry);

    bool executeReplaceRange(const LogEntry & entry);

    /** Updates the queue.
      */
    void queueUpdatingTask();

    void mutationsUpdatingTask();

    /** Clone data from another replica.
      * If replica can not be cloned throw Exception.
      */
    void cloneReplica(const String & source_replica, Coordination::Stat source_is_lost_stat, zkutil::ZooKeeperPtr & zookeeper);

    /// Clone replica if it is lost.
    void cloneReplicaIfNeeded(zkutil::ZooKeeperPtr zookeeper);

    /** Performs actions from the queue.
      */
    BackgroundProcessingPoolTaskResult queueTask();

    /// Perform moves of parts to another disks.
    /// Local operation, doesn't interact with replicationg queue.
    BackgroundProcessingPoolTaskResult movePartsTask();


    /// Postcondition:
    /// either leader_election is fully initialized (node in ZK is created and the watching thread is launched)
    /// or an exception is thrown and leader_election is destroyed.
    void enterLeaderElection();

    /// Postcondition:
    /// is_leader is false, merge_selecting_thread is stopped, leader_election is nullptr.
    /// leader_election node in ZK is either deleted, or the session is marked expired.
    void exitLeaderElection();

    /** Selects the parts to merge and writes to the log.
      */
    void mergeSelectingTask();

    /// Checks if some mutations are done and marks them as done.
    void mutationsFinalizingTask();

    /** Write the selected parts to merge into the log,
      * Call when merge_selecting_mutex is locked.
      * Returns false if any part is not in ZK.
      */
    enum class CreateMergeEntryResult { Ok, MissingPart, LogUpdated, Other };

    CreateMergeEntryResult createLogEntryToMergeParts(
        zkutil::ZooKeeperPtr & zookeeper,
        const DataPartsVector & parts,
        const String & merged_name,
        const MergeTreeDataPartType & merged_part_type,
        bool deduplicate,
        bool force_ttl,
        ReplicatedMergeTreeLogEntryData * out_log_entry,
        int32_t log_version);

    CreateMergeEntryResult createLogEntryToMutatePart(
        const IMergeTreeDataPart & part,
        Int64 mutation_version,
        int32_t alter_version,
        int32_t log_version);

    /// Exchange parts.

    /** Returns an empty string if no one has a part.
      */
    String findReplicaHavingPart(const String & part_name, bool active);

    /** Find replica having specified part or any part that covers it.
      * If active = true, consider only active replicas.
      * If found, returns replica name and set 'entry->actual_new_part_name' to name of found largest covering part.
      * If not found, returns empty string.
      */
    String findReplicaHavingCoveringPart(LogEntry & entry, bool active);
    String findReplicaHavingCoveringPart(const String & part_name, bool active, String & found_part_name);

    /** Download the specified part from the specified replica.
      * If `to_detached`, the part is placed in the `detached` directory.
      * If quorum != 0, then the node for tracking the quorum is updated.
      * Returns false if part is already fetching right now.
      */
    bool fetchPart(const String & part_name, const StorageMetadataPtr & metadata_snapshot, const String & replica_path, bool to_detached, size_t quorum);

    /// Required only to avoid races between executeLogEntry and fetchPartition
    std::unordered_set<String> currently_fetching_parts;
    std::mutex currently_fetching_parts_mutex;


    /// With the quorum being tracked, add a replica to the quorum for the part.
    void updateQuorum(const String & part_name);

    /// Deletes info from quorum/last_part node for particular partition_id.
    void cleanLastPartNode(const String & partition_id);

    /// Creates new block number if block with such block_id does not exist
    std::optional<EphemeralLockInZooKeeper> allocateBlockNumber(
        const String & partition_id, zkutil::ZooKeeperPtr & zookeeper,
        const String & zookeeper_block_id_path = "");

    /** Wait until all replicas, including this, execute the specified action from the log.
      * If replicas are added at the same time, it can not wait the added replica .
      *
      * NOTE: This method must be called without table lock held.
      * Because it effectively waits for other thread that usually has to also acquire a lock to proceed and this yields deadlock.
      * TODO: There are wrong usages of this method that are not fixed yet.
      */
    Strings waitForAllReplicasToProcessLogEntry(const ReplicatedMergeTreeLogEntryData & entry, bool wait_for_non_active = true);

    /** Wait until the specified replica executes the specified action from the log.
      * NOTE: See comment about locks above.
      */
    bool waitForReplicaToProcessLogEntry(const String & replica_name, const ReplicatedMergeTreeLogEntryData & entry, bool wait_for_non_active = true);

    /// Throw an exception if the table is readonly.
    void assertNotReadonly() const;

    /// Produce an imaginary part info covering all parts in the specified partition (at the call moment).
    /// Returns false if the partition doesn't exist yet.
    bool getFakePartCoveringAllPartsInPartition(const String & partition_id, MergeTreePartInfo & part_info);

    /// Check for a node in ZK. If it is, remember this information, and then immediately answer true.
    std::unordered_set<std::string> existing_nodes_cache;
    std::mutex existing_nodes_cache_mutex;
    bool existsNodeCached(const std::string & path);

    /// Remove block IDs from `blocks/` in ZooKeeper for the given partition ID in the given block number range.
    void clearBlocksInPartition(
        zkutil::ZooKeeper & zookeeper, const String & partition_id, Int64 min_block_num, Int64 max_block_num);

    /// Info about how other replicas can access this one.
    ReplicatedMergeTreeAddress getReplicatedMergeTreeAddress() const;

    bool dropPartsInPartition(zkutil::ZooKeeper & zookeeper, String & partition_id,
        StorageReplicatedMergeTree::LogEntry & entry, bool detach);

    // Partition helpers
    void dropPartition(const ASTPtr & query, const ASTPtr & partition, bool detach, const Context & query_context);
    PartitionCommandsResultInfo attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, const Context & query_context);
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & query_context);
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & query_context);
    void fetchPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, const String & from, const Context & query_context);

    /// Check granularity of already existing replicated table in zookeeper if it exists
    /// return true if it's fixed
    bool checkFixedGranualrityInZookeeper();

    /// Wait for timeout seconds mutation is finished on replicas
    void waitMutationToFinishOnReplicas(
        const Strings & replicas, const String & mutation_id) const;

    MutationCommands getFirtsAlterMutationCommandsForPart(const DataPartPtr & part) const override;

    void startBackgroundMovesIfNeeded() override;

protected:
    /** If not 'attach', either creates a new table in ZK, or adds a replica to an existing table.
      */
    StorageReplicatedMergeTree(
        const String & zookeeper_path_,
        const String & replica_name_,
        bool attach,
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        Context & context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        bool has_force_restore_data_flag);
};


/** There are three places for each part, where it should be
  * 1. In the RAM, data_parts, all_data_parts.
  * 2. In the filesystem (FS), the directory with the data of the table.
  * 3. in ZooKeeper (ZK).
  *
  * When adding a part, it must be added immediately to these three places.
  * This is done like this
  * - [FS] first write the part into a temporary directory on the filesystem;
  * - [FS] rename the temporary part to the result on the filesystem;
  * - [RAM] immediately afterwards add it to the `data_parts`, and remove from `data_parts` any parts covered by this one;
  * - [RAM] also set the `Transaction` object, which in case of an exception (in next point),
  *   rolls back the changes in `data_parts` (from the previous point) back;
  * - [ZK] then send a transaction (multi) to add a part to ZooKeeper (and some more actions);
  * - [FS, ZK] by the way, removing the covered (old) parts from filesystem, from ZooKeeper and from `all_data_parts`
  *   is delayed, after a few minutes.
  *
  * There is no atomicity here.
  * It could be possible to achieve atomicity using undo/redo logs and a flag in `DataPart` when it is completely ready.
  * But it would be inconvenient - I would have to write undo/redo logs for each `Part` in ZK, and this would increase already large number of interactions.
  *
  * Instead, we are forced to work in a situation where at any time
  *  (from another thread, or after server restart), there may be an unfinished transaction.
  *  (note - for this the part should be in RAM)
  * From these cases the most frequent one is when the part is already in the data_parts, but it's not yet in ZooKeeper.
  * This case must be distinguished from the case where such a situation is achieved due to some kind of damage to the state.
  *
  * Do this with the threshold for the time.
  * If the part is young enough, its lack in ZooKeeper will be perceived optimistically - as if it just did not have time to be added there
  *  - as if the transaction has not yet been executed, but will soon be executed.
  * And if the part is old, its absence in ZooKeeper will be perceived as an unfinished transaction that needs to be rolled back.
  *
  * PS. Perhaps it would be better to add a flag to the DataPart that a part is inserted into ZK.
  * But here it's too easy to get confused with the consistency of this flag.
  */
#define MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER (5 * 60)

}
