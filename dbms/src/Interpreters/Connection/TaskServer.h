//
// Created by jungle on 19-6-17.
//

#pragma once


#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/TCPServer.h>
#include <Server/IServer.h>

namespace DB {

 // receive task and fork process to deal(DataServer will be created in new process)
    class ConcurrentBoundedQueue ;
    class  DataReceiverInfo ;
    class TaskServer  : public IServer{

    public:
        TaskServer(Poco::Net::TCPServerConnectionFactory * connectionFactory_,int port):
                connectionFactory(connectionFactory_),portNum(port){
            server = std::make_unique<Poco::Net::TCPServer>(connectionFactory_,portNum);
        }
        void start() {
            server->start();
        }
        bool  isCancelled() const override;
        DataReceiverInfo  applyResource();

    private:
        int portNum ;
        std::unique_ptr<Poco::Net::TCPServer> server;
        Poco::Net::TCPServerConnectionFactory * connectionFactory;
        std::map<std::string,std::shared_ptr<ConcurrentBoundedQueue<Block>> resultTaskBuffer;



    };

}


