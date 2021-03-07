#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

namespace
{

using namespace DB;

Field convertField(const ASTIdentifier & identifier, const Field & value, const ExpressionActionsPtr & expr)
{
    for (const auto & name_and_type : expr->getRequiredColumnsWithTypes())
    {
        const auto & name = name_and_type.name;
        const auto & type = name_and_type.type;

        if (name == identifier.name())
        {
            Field converted = convertFieldToType(value, *type);
            if (converted.isNull())
                return {};
            return converted;
        }
    }

    return {};
}

/// Return true if shard may contain such value (or it is unknown), otherwise false.
bool shardContains(
    const ASTIdentifier & identifier, Field field, const ExpressionActionsPtr & expr,
    const Cluster::ShardInfo & shard_info, const Cluster::SlotToShard & slots)
{
    field = convertField(identifier, field, expr);
    if (field.isNull())
        return true;

    UInt64 value = field.get<UInt64>();
    const auto shard_num = slots[value % slots.size()] + 1;
    return shard_info.shard_num == shard_num;
}

}

namespace DB
{

bool OptimizeShardingKeyRewriteInMatcher::needChildVisit(ASTPtr & /*node*/, const ASTPtr & /*child*/)
{
    return true;
}

void OptimizeShardingKeyRewriteInMatcher::visit(ASTPtr & node, Data & data)
{
    if (auto * function = node->as<ASTFunction>())
        visit(*function, data);
}

void OptimizeShardingKeyRewriteInMatcher::visit(ASTFunction & function, Data & data)
{
    if (function.name != "in")
        return;

    auto * left = function.arguments->children.front().get();
    auto * right = function.arguments->children.back().get();
    auto * identifier = left->as<ASTIdentifier>();
    if (!identifier)
        return;

    const auto & expr = data.sharding_key_expr;

    /// NOTE: that we should not take care about empty tuple,
    /// since after optimize_skip_unused_shards,
    /// at least one element should match each shard.
    if (auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple")
    {
        auto * tuple_elements = tuple_func->children.front()->as<ASTExpressionList>();
        std::erase_if(tuple_elements->children, [&](auto & child)
        {
            auto * literal = child->template as<ASTLiteral>();
            return literal && !shardContains(*identifier, literal->value, expr, data.shard_info, data.slots);
        });
    }
    else if (auto * tuple_literal = right->as<ASTLiteral>();
        tuple_literal && tuple_literal->value.getType() == Field::Types::Tuple)
    {
        auto & tuple = tuple_literal->value.get<Tuple &>();
        std::erase_if(tuple, [&](auto & child)
        {
            return !shardContains(*identifier, child, expr, data.shard_info, data.slots);
        });
    }
}

}
