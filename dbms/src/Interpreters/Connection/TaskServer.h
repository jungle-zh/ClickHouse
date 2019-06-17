//
// Created by jungle on 19-6-17.
//

#pragma once


namespace DB {

 // receive task and fork process to deal(DataServer will be created in new process)
    class TaskServer  {

    public:
        TaskServer(Poco::Net::TCPServerConnectionFactory * connectionFactory_,int port):
                connectionFactory(connectionFactory_),portNum(port){
            server = std::make_unique<Poco::Net::TCPServer>(connectionFactory_,portNum);
        }
        void start() {
            server->start();
        }


    private:
        int portNum ;
        std::unique_ptr<Poco::Net::TCPServer> server;
        Poco::Net::TCPServerConnectionFactory * connectionFactory;

    };

}


