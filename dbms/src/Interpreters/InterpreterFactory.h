#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Core/Protocol.h>


namespace DB
{

class Context;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


class InterpreterFactory
{
public:
    static std::unique_ptr<IInterpreter> get(
        ASTPtr & query,
        Context & context,
        QueryProcessingStage::Enum stage = QueryProcessingStage::Complete,
        Protocol::Client::Enum query_type = Protocol::Client::Query,
        std::shared_ptr<std::map<String,StoragePtr >> shuffle_table = nullptr);
};

}
