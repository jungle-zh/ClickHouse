//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServer.h>
#include <Server/IServer.h>
#include "DataConnectionHandlerFactory.h"

namespace DB {

class DataReceiver;
class DataServer : public IServer {

public:
    DataServer(int port,DataReceiver * receiver1):
    portNum(port),receiver_(receiver1){
        connectionFactory = std::make_unique<DataConnectionHandlerFactory>();
        server = std::make_unique<Poco::Net::TCPServer>(connectionFactory,portNum);

        connectionFactory->setServer(this);

    }
    void start() {
        server->start();
    }
    void fill(Block & block);

    bool  isCancelled() const override;

    DataReceiver * receiver() { return receiver_; }

private:
    int portNum ;
    DataReceiver * receiver_;
    std::unique_ptr<Poco::Net::TCPServer> server;
    std::unique_ptr<DataConnectionHandlerFactory > connectionFactory;

};

}

