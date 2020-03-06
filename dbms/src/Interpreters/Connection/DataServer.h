//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServer.h>
#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <map>
#include <common/logger_useful.h>

//#include "DataConnectionHandlerFactory.h"

namespace DB {

class DataConnectionHandlerFactory;
class DataExechangeServer;
class Context ;
class DataConnectionHandler;
class Block;
class Task ;
class DataServer  {

public:

    DataServer(std::map<UInt32 ,std::shared_ptr<ConcurrentBoundedQueue<Block>> > buffer,
            UInt32 port,Context * context,Task * task);

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
    std::shared_ptr<ConcurrentBoundedQueue<Block>> getBufferByPartition(size_t partitionId);

private:

    std::map<UInt32 ,std::shared_ptr<ConcurrentBoundedQueue<Block>> > partitionBuffer;

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

