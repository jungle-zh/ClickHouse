//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Core/Protocol.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <common/ThreadPool.h>
//#include <Interpreters/Connection/DataServer.h>

namespace DB {

class ReadBuffer;
class WriteBuffer ;
class IBlockInputStream;
class IServer;
class Context;
class DataConnectionHandler : public Poco::Net::TCPServerConnection {


private:

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    //std::shared_ptr<NativeBlockInputStream> block_in;
    //std::shared_ptr<NativeBlockOutputStream> block_out;

    Protocol::Compression compression = Protocol::Compression::Disable;
    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    std::shared_ptr<IBlockInputStream> block_in;

    DataServer * server;
    std::string senderId ; // senderId is set in receive hello;


    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_revision = 0;
    std::string child_task_id ;
    int upstream_task_partition;

    Poco::Logger * log;
    Context * connection_context;

    std::exception_ptr exception;

public:
    DataConnectionHandler(const Poco::Net::StreamSocket & socket_,DataServer * server_);


    void run() override;
    void initBlockInput();
    void runImpl(); // receive data and fill

    bool receiveBlock();
    void receiveHello();

    void setStartToReceive(bool start) { startToReceive = start;}
    //bool getStartToReceive() { return  startToReceive;}

    void setEndOfReceive(bool end)  { endOfReceive = end;}


    bool startToReceive = false;
    bool endOfReceive = false;
    UInt64 recievedRows ;
    std::function<void(Block & ,std::string ) >  receiveBlockCall;
    std::function<void(std::string)> finishCall;
    std::function<bool ()> highWaterMarkCall;

    ThreadPool pool{1};
    void sendCommandToClient(Protocol::DataControl::Enum  type);

    void checkHighWaterMark();

    bool getEndOfReceive();

    void  sendException(const Exception & e) ;
    void  sendEndOfStream();
    std::string childTaskId(){ return  child_task_id;}
};


}