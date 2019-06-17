//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServer.h>

namespace DB {


class DataServer  {

public:
    DataServer(Poco::Net::TCPServerConnectionFactory * connectionFactory_,int port):
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

