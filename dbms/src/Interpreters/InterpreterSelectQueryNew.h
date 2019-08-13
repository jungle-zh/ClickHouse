//
// Created by jungle on 19-6-22.
//

#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include "QueryAnalyzer.h"
#include "TaskScheduler.h"


namespace DB {


class InterpreterSelectQueryNew  : public IInterpreter {

public:
        /// Note: the query is cloned because it will be modified during analysis.


    InterpreterSelectQueryNew(ASTPtr query_ptr_ , Context * context_):
            query_ptr(query_ptr_->clone()),
            unionQuery(typeid_cast<ASTSelectWithUnionQuery &>(*query_ptr)),
            query(typeid_cast<ASTSelectQuery &>(*(unionQuery.list_of_selects->children[0]))),
            context(context_){
        queryAnalyzer = std::make_unique<QueryAnalyzer>(context_,"jobid");
        taskScheduler = std::make_unique<TaskScheduler>();
    }
    ASTPtr query_ptr;

    ASTSelectWithUnionQuery & unionQuery;
    ASTSelectQuery & query;
    Context * context;

    size_t subquery_depth;
    std::unique_ptr<QueryAnalyzer> queryAnalyzer;
    std::unique_ptr<TaskScheduler> taskScheduler;

public:
    BlockIO  execute() override;
    TaskScheduler * getScheduler() { return  taskScheduler.get();}

};


}
