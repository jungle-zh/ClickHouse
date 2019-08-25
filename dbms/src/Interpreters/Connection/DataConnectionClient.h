//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/CompressionSettings.h>
#include <common/ThreadPool.h>

namespace DB {




    class  Task;
    class DataConnectionClient {




    public:

        DataConnectionClient(std::string ip_, UInt16  port_,Task * task_,Context * context_ );


        bool connected = false;
        std::shared_ptr<IBlockInputStream> block_in;
        std::shared_ptr<IBlockOutputStream> block_out; //NativeBlockOutputStream
        std::shared_ptr<ReadBuffer> maybe_compressed_in;
        //std::shared_ptr<WriteBuffer> maybe_compressed_out;
        Protocol::Compression compression;
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        std::shared_ptr<WriteBuffer> out;




        UInt64 server_revision = 0;
        Poco::Net::SocketAddress resolved_address;
        ConnectionTimeouts timeouts;

        std::string ip;
        UInt16 port ;
        Settings settings;
        CompressionSettings  compression_settings ;

        bool connect();
        void disconnect();

       // bool receiveStopCommand();
       // void sendBlock(Block & block); // logic thread call
       // void sendHello(std::string taskId,int partionId); // send taskId(include stageId)
        void sendPartitionId(std::string taskId,size_t partionId);
        Block receiveBlock();
        Block read();
        void receiveHello();
        int  stageId ;
        void startListen();
        void listen();

        std::thread  listener; // listen remote command

        Poco::Logger * log;
        bool finished = false;
        Task * task;
        Context * context;


    };


}


