//
// Created by jungle on 19-6-17.
//

#include "DataServer.h"
#include <Interpreters/DataExechangeServer.h>
#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include <Interpreters/Connection/DataConnectionHandler.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Block.h>


namespace DB {


    DataServer::DataServer(std::map<UInt32 ,std::shared_ptr<ConcurrentBoundedQueue<Block>> > buffer,UInt32 port,Context * context,Task * task_){
        partitionBuffer = buffer;
        portNum = port;
        context_ = context;
        connectionFactory = new DataConnectionHandlerFactory();

        server = std::make_unique<Poco::Net::TCPServer>(connectionFactory,portNum);
        static_cast<DataConnectionHandlerFactory *>(connectionFactory)->setDataServer(this);
        task = task_;
        log = &Logger::get("DataReceiver");
    }

    void DataServer::addConnection(DataConnectionHandler * conn) {


        connections_.insert({conn->fatherTaskId(),conn}) ;
        task->addChildTask(conn->fatherTaskId());
        LOG_INFO(log,"task :" + task->getTaskId() + " receive data connection from child task :" +  conn->fatherTaskId());
    }
    std::shared_ptr<ConcurrentBoundedQueue<Block>> DataServer::getBufferByPartition(size_t partitionId){

        auto it = partitionBuffer.find(partitionId);
        if(it  == partitionBuffer.end()){
            throw Exception("not find partitionBuffer");
        }
        return partitionBuffer[partitionId];
    }


}