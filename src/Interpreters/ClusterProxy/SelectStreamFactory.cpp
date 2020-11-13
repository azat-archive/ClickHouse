#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/checkStackSize.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <IO/ConnectionTimeoutsContext.h>

#include <common/logger_useful.h>
#include <Processors/Pipe.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
    const Block & header_,
    QueryProcessingStage::Enum processed_stage_,
    StorageID main_table_,
    const Scalars & scalars_,
    bool has_virtual_shard_num_column_,
    const Tables & external_tables_)
    : header(header_),
    processed_stage{processed_stage_},
    main_table(std::move(main_table_)),
    table_func_ptr{nullptr},
    scalars{scalars_},
    has_virtual_shard_num_column(has_virtual_shard_num_column_),
    external_tables{external_tables_}
{
}

SelectStreamFactory::SelectStreamFactory(
    const Block & header_,
    QueryProcessingStage::Enum processed_stage_,
    ASTPtr table_func_ptr_,
    const Scalars & scalars_,
    bool has_virtual_shard_num_column_,
    const Tables & external_tables_)
    : header(header_),
    processed_stage{processed_stage_},
    table_func_ptr{table_func_ptr_},
    scalars{scalars_},
    has_virtual_shard_num_column(has_virtual_shard_num_column_),
    external_tables{external_tables_}
{
}

namespace
{

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    const Context & context,
    QueryProcessingStage::Enum processed_stage)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();

    InterpreterSelectQuery interpreter(query_ast, context, SelectQueryOptions(processed_stage));
    interpreter.buildQueryPlan(*query_plan);

    /// Convert header structure to expected.
    /// Also we ignore constants from result and replace it with constants from header.
    /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            query_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
            header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            true);

    auto converting = std::make_unique<ExpressionStep>(query_plan->getCurrentDataStream(), convert_actions_dag);
    converting->setStepDescription("Convert block structure for query from local replica");
    query_plan->addStep(std::move(converting));

    return query_plan;
}

String formattedAST(const ASTPtr & ast)
{
    if (!ast)
        return {};
    WriteBufferFromOwnString buf;
    formatAST(*ast, buf, false, true);
    return buf.str();
}

/// Rewrites '_shard_num AS foo' to '_shard_num'
//
/// Since iniator expects column '_shard_num' not 'foo', since it does not
/// expect from the shard to make projections, see also rewriteShardNum()
class RewriteShardNumMatcher
{
public:
    struct Data
    {
        std::string alias_name;
    };

    static bool needChildVisit(ASTPtr & /*node*/, const ASTPtr & /*child*/)
    {
        return true;
    }

    static void visit(ASTPtr & node, Data &data)
    {
        if (auto * identifier = node->as<ASTIdentifier>())
        {
            if (data.alias_name.empty() && identifier->shortName() == "_shard_num")
            {
                data.alias_name = identifier->alias;
                identifier->setAlias("");
            }
            else if (!data.alias_name.empty())
            {
                if (identifier->alias == data.alias_name)
                    identifier->setAlias("");
            }
        }
    }
};

void rewriteShardNum(ASTPtr & ast, const Field & shard_num)
{
    /// In case _shard_num was requested with an alias the alias will be
    /// normalized by the QueryNormalizer and final alias will be used on
    /// shards (not _shard_num), but on the initiator _shard_num will not be
    /// normalized, since it is the parent alias (due to lack of WITH
    /// statement there)
    ///
    /// So the alias for _shard_num should be replaced with _shard_num itself
    /// (to match the query on the iniator and the shard).
    ///
    /// NOTE: that regular WITH from Distributed
    /// (i.e. WITH 1 AS foo SELECT * FROM remote('127.{1,2}') GROUP BY foo)
    /// is fine, since `foo` is the same on the initiator and shard (while
    /// _shard_num is parent on the initiator and child on shards).
    RewriteShardNumMatcher::Data visitor_data;
    InDepthNodeVisitor<RewriteShardNumMatcher, true> visitor(visitor_data);
    visitor.visit(ast);
    /// Run second time to replace missing aliases
    if (!visitor_data.alias_name.empty())
        visitor.visit(ast);

    /// toUInt32 to match type with system.clusters.shard_num
    VirtualColumnUtils::rewriteEntityInAst(ast, "_shard_num", shard_num, "toUInt32");
}

}

void SelectStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const String &, const ASTPtr & query_ast,
    const std::shared_ptr<Context> & context_ptr, const ThrottlerPtr & throttler,
    const SelectQueryInfo &,
    std::vector<QueryPlanPtr> & plans,
    Pipes & remote_pipes,
    Pipes & delayed_pipes,
    Poco::Logger * log)
{
    const auto & context = *context_ptr;

    bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;
    bool add_totals = false;
    bool add_extremes = false;
    bool async_read = context_ptr->getSettingsRef().async_socket_for_remote;
    if (processed_stage == QueryProcessingStage::Complete)
    {
        add_totals = query_ast->as<ASTSelectQuery &>().group_by_with_totals;
        add_extremes = context.getSettingsRef().extremes;
    }

    auto modified_query_ast = query_ast->clone();
    if (has_virtual_shard_num_column)
        rewriteShardNum(modified_query_ast, shard_info.shard_num);

    auto emplace_local_stream = [&]()
    {
        plans.emplace_back(createLocalPlan(modified_query_ast, header, context, processed_stage));
    };

    String modified_query = formattedAST(modified_query_ast);

    auto emplace_remote_stream = [&]()
    {
        auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
            shard_info.pool, modified_query, header, context, throttler, scalars, external_tables, processed_stage);
        remote_query_executor->setLogger(log);

        remote_query_executor->setPoolMode(PoolMode::GET_MANY);
        if (!table_func_ptr)
            remote_query_executor->setMainTable(main_table);

        remote_pipes.emplace_back(createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read));
        remote_pipes.back().addInterpreterContext(context_ptr);
    };

    const auto & settings = context.getSettingsRef();

    if (settings.prefer_localhost_replica && shard_info.isLocal())
    {
        StoragePtr main_table_storage;

        if (table_func_ptr)
        {
            TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().get(table_func_ptr, context);
            main_table_storage = table_function_ptr->execute(table_func_ptr, context, table_function_ptr->getName());
        }
        else
        {
            auto resolved_id = context.resolveStorageID(main_table);
            main_table_storage = DatabaseCatalog::instance().tryGetTable(resolved_id, context);
        }


        if (!main_table_storage) /// Table is absent on a local server.
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            if (shard_info.hasRemoteConnections())
            {
                LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                    "There is no table {} on local replica of shard {}, will try remote replicas.",
                    main_table.getNameForLogs(), shard_info.shard_num);
                emplace_remote_stream();
            }
            else
                emplace_local_stream();  /// Let it fail the usual way.

            return;
        }

        const auto * replicated_storage = dynamic_cast<const StorageReplicatedMergeTree *>(main_table_storage.get());

        if (!replicated_storage)
        {
            /// Table is not replicated, use local server.
            emplace_local_stream();
            return;
        }

        UInt64 max_allowed_delay = settings.max_replica_delay_for_distributed_queries;

        if (!max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        UInt32 local_delay = replicated_storage->getAbsoluteDelay();

        if (local_delay < max_allowed_delay)
        {
            emplace_local_stream();
            return;
        }

        /// If we reached this point, local replica is stale.
        ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"), "Local replica of shard {} is stale (delay: {}s.)", shard_info.shard_num, local_delay);

        if (!settings.fallback_to_stale_replicas_for_distributed_queries)
        {
            if (shard_info.hasRemoteConnections())
            {
                /// If we cannot fallback, then we cannot use local replica. Try our luck with remote replicas.
                emplace_remote_stream();
                return;
            }
            else
                throw Exception(
                    "Local replica of shard " + toString(shard_info.shard_num)
                    + " is stale (delay: " + toString(local_delay) + "s.), but no other replica configured",
                    ErrorCodes::ALL_REPLICAS_ARE_STALE);
        }

        if (!shard_info.hasRemoteConnections())
        {
            /// There are no remote replicas but we are allowed to fall back to stale local replica.
            emplace_local_stream();
            return;
        }

        /// Try our luck with remote replicas, but if they are stale too, then fallback to local replica.
        /// Do it lazily to avoid connecting in the main thread.

        auto lazily_create_stream = [
                pool = shard_info.pool, shard_num = shard_info.shard_num, modified_query, header = header, modified_query_ast,
                &context, context_ptr, throttler,
                main_table = main_table, table_func_ptr = table_func_ptr, scalars = scalars, external_tables = external_tables,
                stage = processed_stage, local_delay, add_agg_info, add_totals, add_extremes, async_read]()
            -> Pipe
        {
            auto current_settings = context.getSettingsRef();
            auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(
                current_settings).getSaturated(
                    current_settings.max_execution_time);
            std::vector<ConnectionPoolWithFailover::TryResult> try_results;
            try
            {
                if (table_func_ptr)
                    try_results = pool->getManyForTableFunction(timeouts, &current_settings, PoolMode::GET_MANY);
                else
                    try_results = pool->getManyChecked(timeouts, &current_settings, PoolMode::GET_MANY, main_table.getQualifiedName());
            }
            catch (const Exception & ex)
            {
                if (ex.code() == ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
                    LOG_WARNING(&Poco::Logger::get("ClusterProxy::SelectStreamFactory"),
                        "Connections to remote replicas of local shard {} failed, will use stale local replica", shard_num);
                else
                    throw;
            }

            double max_remote_delay = 0.0;
            for (const auto & try_result : try_results)
            {
                if (!try_result.is_up_to_date)
                    max_remote_delay = std::max(try_result.staleness, max_remote_delay);
            }

            if (try_results.empty() || local_delay < max_remote_delay)
            {
                auto plan = createLocalPlan(modified_query_ast, header, context, stage);
                return QueryPipeline::getPipe(std::move(*plan->buildQueryPipeline()));
            }
            else
            {
                std::vector<IConnectionPool::Entry> connections;
                connections.reserve(try_results.size());
                for (auto & try_result : try_results)
                    connections.emplace_back(std::move(try_result.entry));

                auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
                    std::move(connections), modified_query, header, context, throttler, scalars, external_tables, stage);

                return createRemoteSourcePipe(remote_query_executor, add_agg_info, add_totals, add_extremes, async_read);
            }
        };

        delayed_pipes.emplace_back(createDelayedPipe(header, lazily_create_stream, add_totals, add_extremes));
        delayed_pipes.back().addInterpreterContext(context_ptr);
    }
    else
        emplace_remote_stream();
}

}
}
