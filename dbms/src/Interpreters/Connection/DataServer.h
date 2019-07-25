//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServer.h>

//#include "DataConnectionHandlerFactory.h"

namespace DB {

class DataConnectionHandlerFactory;
class DataReceiver;
class Context ;
class DataServer  {

public:
    DataServer(int port):
    portNum(port){
        connectionFactory = std::make_unique<DataConnectionHandlerFactory>(this);
        server = std::make_unique<Poco::Net::TCPServer>(connectionFactory.get(),portNum);

        //connectionFactory->setServer(this);

    }
    void start() {
        server->start();
    }

    bool getStartToReceive();

    //DataReceiver * receiver() { return receiver_; }
    int portNum ;

private:

    Context * global_context;
    std::unique_ptr<Poco::Net::TCPServer> server;
    std::unique_ptr<DataConnectionHandlerFactory > connectionFactory;

};

}

