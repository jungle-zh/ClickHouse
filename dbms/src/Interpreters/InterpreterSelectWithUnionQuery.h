#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Core/Protocol.h>

namespace DB
{

class InterpreterSelectQuery;
class InterpreterEnhanceJoinSelectQuery;

/** Interprets one or multiple SELECT queries inside UNION ALL chain.
  */
class InterpreterSelectWithUnionQuery : public IInterpreter
{
public:
    InterpreterSelectWithUnionQuery(
        const ASTPtr & query_ptr_,
        const Context & context_,
        const Names & required_result_column_names = Names{},
        QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
        size_t subquery_depth_ = 0,
        Protocol::Client::Enum query_type = Protocol::Client::Query,
        std::shared_ptr<std::map<String,StoragePtr >> shuffle_table = nullptr );

    ~InterpreterSelectWithUnionQuery();

    BlockIO execute() override;

    /// Execute the query without union of streams.
    BlockInputStreams executeWithMultipleStreams();

    Block getSampleBlock();

    static Block getSampleBlock(
        const ASTPtr & query_ptr_,
        const Context & context_);

    void ignoreWithTotals();

private:
    ASTPtr query_ptr;
    Context context;
    QueryProcessingStage::Enum to_stage;
    size_t subquery_depth;

    Protocol::Client::Enum query_type;
    std::shared_ptr<std::map<String,StoragePtr >> shuffle_table;
    //std::vector<std::unique_ptr<InterpreterSelectQuery>> nested_interpreters;
    std::vector<std::unique_ptr<InterpreterEnhanceJoinSelectQuery>> nested_interpreters;
    Block result_header;
};

}
