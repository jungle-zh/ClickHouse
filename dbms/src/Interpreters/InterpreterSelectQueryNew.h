//
// Created by jungle on 19-6-22.
//

#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/typeid_cast.h>
#include "QueryAnalyzer.h"
#include "TaskScheduler.h"


namespace DB {


class InterpreterSelectQueryNew  : public IInterpreter {

private:
        /// Note: the query is cloned because it will be modified during analysis.


    InterpreterSelectQueryNew(ASTPtr query_ptr_ , Context * context_):
            query_ptr(query_ptr_->clone()),
            query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
            context(context_){
    }
    ASTPtr query_ptr;
    ASTSelectQuery & query;
    Context * context;

    size_t subquery_depth;
    std::unique_ptr<QueryAnalyzer> queryAnalyzer;
    std::unique_ptr<TaskScheduler> taskScheduler;

public:
    BlockIO  execute() override;


};


}
