//
// Created by jungle on 19-6-17.
//

#pragma once



#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Block.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/TCPServer.h>
#include "DataConnectionHandlerFactory.h"

namespace DB {

 // receive task and fork process to deal(DataServer will be created in new process)

    class DataServer;
    class Context ;
    struct DataReceiverInfo ;
    class TaskServer  {

    public:
        TaskServer(Poco::Net::TCPServerConnectionFactory * connectionFactory_,UInt32 port,Context * context);
        void start() {
            server->start();
        }
        Context * context(){ return  server_context;}
        bool  isCancelled(){ return false;}
        //DataReceiverInfo  applyResource();
        TaskSource getExechangeServerInfo() ;
        //DataConnectionHandlerFactory * getDataConnectionHandlerFactory(){ return  dataFactory;}

    private:
        Poco::Net::TCPServerConnectionFactory * connectionFactory;
        UInt32 portNum ;
        std::unique_ptr<Poco::Net::TCPServer> server;
        std::map<std::string,std::shared_ptr<ConcurrentBoundedQueue<Block>>> resultTaskBuffer;
        //std::unique_ptr<DataConnectionHandlerFactory> dataFactory;
        //DataConnectionHandlerFactory * dataFactory;
        //std::vector<std::shared_ptr<DataServer>>  dataServers;
        Poco::Logger * log;
        Context * server_context ;
        UInt16 dataPortCnt ;

    };

}


