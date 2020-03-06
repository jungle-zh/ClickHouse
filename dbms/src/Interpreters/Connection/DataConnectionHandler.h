//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Core/Protocol.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <common/ThreadPool.h>
//#include <Interpreters/Connection/DataServer.h>
#include <Common/ConcurrentBoundedQueue.h>
namespace DB {

class ReadBuffer;
class WriteBuffer ;
class IBlockInputStream;
class IBlockOutputStream;

class Block;
class Task;
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
    std::shared_ptr<IBlockOutputStream> block_out;
    //std::vector<Block> send_buffer;



    DataServer * server;
    std::string senderId ; // senderId is set in receive hello;

    Task * task ;
    String client_name;
    UInt64 client_version_major = 0;
    UInt64 client_version_minor = 0;
    UInt64 client_revision = 0;
    std::string father_task_id ;

    size_t poped_cnt  ;
    Poco::Logger * log;
    Context * connection_context;

    std::exception_ptr exception;
    bool  finished = false;
    std::string dataChannel ; // stageId_curShuffleKey_partionNum_destShuffleKey_partionNum;

    std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer ; // partitioned buffer
    size_t father_task_partition;
public:

    DataConnectionHandler(const Poco::Net::StreamSocket & socket_,DataServer * server_);
    //std::function<()> setBufferCall;

    void run() override;
    void initBlockInput();
    void runImpl(); // receive data and fill

    bool receiveBlock();
    //void receiveHello();
    void receivePartitionID();
    void receivePackage();


    void sendBlock(DB::Block &block);


    //bool startToReceive = false;
    //bool endOfReceive = false;
    UInt64 recievedRows ;
    std::function<void(Block & ,std::string ) >  receiveBlockCall;
    std::function<void(std::string)> finishCall;
    std::function<bool ()> highWaterMarkCall;

    ThreadPool pool{1};
    //void sendCommandToClient(Protocol::DataControl::Enum  type);

    //void checkHighWaterMark();

   // bool getEndOfReceive();

    void  sendException(const Exception & e) ;
    void  sendEndOfStream();
    std::string fatherTaskId(){ return  father_task_id;}
};


}