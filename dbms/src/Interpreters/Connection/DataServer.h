//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServer.h>
#include <Core/Types.h>
#include <map>
#include <common/logger_useful.h>

//#include "DataConnectionHandlerFactory.h"

namespace DB {

class DataConnectionHandlerFactory;
class DataReceiver;
class Context ;
class DataConnectionHandler;
class Task ;
class DataServer  {

public:

    DataServer(UInt32 port,Context * context,Task * task);

    void start() {
        server->start();
    }
    void addConnection(DataConnectionHandler * conn);

    bool getStartToReceive();

    //DataReceiver * receiver() { return receiver_; }


    bool  isCancelled(){ return false;}
    Context * context(){ return  context_;}
    Task * getTask() { return  task;}
    std::map<std::string,DataConnectionHandler * > connections(){ return  connections_;}

private:


    std::unique_ptr<Poco::Net::TCPServer> server;
    //std::unique_ptr<Poco::Net::TCPServerConnectionFactory > connectionFactory;
    Poco::Net::TCPServerConnectionFactory * connectionFactory;
    std::map<std::string,DataConnectionHandler * >  connections_ ;
    UInt32 portNum ;
    Context * context_;
    Task * task ;
    Poco::Logger *log;

};

}

