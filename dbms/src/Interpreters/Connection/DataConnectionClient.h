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


        DataConnectionClient(std::string ip_, UInt32  port_,Task * task_ ):resolved_address(ip_,port_){

            ip = ip_;
            port = port_;
            log = &Poco::Logger::get("DataConnectionClient");
            task = task_;
            //settings = settings_;
            //compression_settings =  CompressionSettings(settings) ;
        }

        bool connected = false;
        std::shared_ptr<IBlockOutputStream> block_out; //NativeBlockOutputStream

        std::shared_ptr<WriteBuffer> maybe_compressed_out;
        Protocol::Compression compression;
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        std::shared_ptr<WriteBuffer> out;




        UInt64 server_revision = 0;
        Poco::Net::SocketAddress resolved_address;
        ConnectionTimeouts timeouts;

        std::string ip;
        UInt32 port ;
        Settings settings;
        CompressionSettings  compression_settings ;

        bool connect();
        void disconnect();

        bool receiveStopCommand();
        void sendBlock(Block & block); // logic thread call
        void sendHello(std::string taskId,int partionId); // send taskId(include stageId)
        void receiveHello();
        int  stageId ;
        void startListen();
        void listen();

        std::thread  listener; // listen remote command

        Poco::Logger * log;
        bool stopSend = true;
        Task * task;
        ThreadPool pool{1};

    };


}


