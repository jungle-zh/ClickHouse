//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Interpreters/Connection/DataServer.h>

namespace DB {

class DataServer;
class DataConnectionHandler : public  Poco::Net::TCPServerConnection {


private:



    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    //std::shared_ptr<NativeBlockInputStream> block_in;
    //std::shared_ptr<NativeBlockOutputStream> block_out;

    Protocol::Compression compression = Protocol::Compression::Disable;
    /// From where to read data for INSERT.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;

    DataServer * server;
    std::string senderId ; // senderId is set in receive hello;


    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_revision = 0;
    std::string upstream_task_id ;
    int upstream_task_partition;

    Poco::Logger * log;
    Context connection_context;

public:
    DataConnectionHandler(const Poco::Net::StreamSocket & socket_,DataServer * server_)
    :Poco::Net::TCPServerConnection(socket_),
    server(server_), connection_context(server_->context()) {

        log = &Poco::Logger::get("DataConnectionHandler");

        block_in = std::make_shared<NativeBlockInputStream>(std::make_shared<ReadBufferFromPocoSocket>(socket()),1);
        //block_out = std::make_shared<NativeBlockOutputStream>(std::make_shared<WriteBufferFromPocoSocket>(socket()),1);

    };
    void initBlockInput();
    void runImpl(); // receive data and fill

    bool receiveBlock();
    void receiveHello();

    void setStartToReceive(bool start) { startToReceive = start;}
    //bool getStartToReceive() { return  startToReceive;}

    void setEndOfReceive(bool end)  { endOfReceive = end;}
    //bool getEndOfReceive() { return endOfReceive;}

    bool startToReceive = false;
    bool endOfReceive = false;
    std::function<void(Block & ) >  receiveBlockCall;
    std::function<bool ()> highWaterMarkCall;


    bool getEndOfReceive();
};


}