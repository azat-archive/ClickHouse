#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>

namespace DB
{

/// previousQueryId() - returns query_id for the previous query
class FunctionPreviousQueryId : public IFunction
{
private:
    String query_id;

public:
    static constexpr auto name = "previousQueryId";
    static FunctionPtr create(const Context & context)
    {
        auto function = std::make_shared<FunctionPreviousQueryId>();
        function->query_id = context.getPreviousQueryId();
        return function;
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, query_id);
    }
};


void registerFunctionPreviousQueryId(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPreviousQueryId>();
}

}
