//
// Created by jungle on 19-6-17.
//

#include <Interpreters/Partition.h>
#include <common/logger_useful.h>
#include <Interpreters/Context.h>
#include "TaskServer.h"
#include "DataServer.h"
#include "DataConnectionHandlerFactory.h"

namespace DB{

    TaskServer::TaskServer(Poco::Net::TCPServerConnectionFactory * connectionFactory_
    ,UInt32 port
    ,Context * context_):
    connectionFactory(connectionFactory_),portNum(port){
        server = std::make_unique<Poco::Net::TCPServer>(connectionFactory_,portNum);
        //dataFactory = std::make_unique<DataConnectionHandlerFactory>();
        //dataFactory = context_->getDataConnectionHandlerFactory();
        log = &Logger::get("TaskServer");
        server_context = context_;
        LOG_INFO(log,"TaskServer started ..");
    }
    DataReceiverInfo TaskServer::applyResource() {
        DataReceiverInfo info;
        info.ip = "localhost";
        info.dataPort = 7000;
        return  info;
    }
}