//
// Created by jungle on 19-6-22.
//

#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTSelectQuery.h>
#include "QueryAnalyzer.h"
#include "TaskScheduler.h"


namespace DB {


class InterpreterSelectQueryNew  : public IInterpreter {

private:

    ASTPtr query_ptr;
    ASTSelectQuery & query;
    Context  context;

    size_t subquery_depth;
    std::unique_ptr<QueryAnalyzer> queryAnalyzer;
    std::unique_ptr<TaskScheduler> taskScheduler;

public:
    void  execute(std::string destIp ,int destPort) override;


};


}
