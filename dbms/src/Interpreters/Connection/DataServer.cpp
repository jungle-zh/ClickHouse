//
// Created by jungle on 19-6-17.
//

#include "DataServer.h"
#include <Interpreters/DataReceiver.h>
#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include <Interpreters/Connection/DataConnectionHandler.h>


namespace DB {


    DataServer::DataServer(UInt32 port,Context * context,Task * task_):
    portNum(port){
            context_ = context;
            connectionFactory = new DataConnectionHandlerFactory();

            server = std::make_unique<Poco::Net::TCPServer>(connectionFactory,portNum);
            static_cast<DataConnectionHandlerFactory *>(connectionFactory)->setDataServer(this);
        task = task_;
        log = &Logger::get("DataReceiver");
    }

    void DataServer::addConnection(DataConnectionHandler * conn) {
        connections_.insert({conn->childTaskId(),conn}) ;
        task->addChildTask(conn->childTaskId());
        LOG_INFO(log,"task :" + task->getTaskId() + " receive data connection from child task :" +  conn->childTaskId());
    }


}