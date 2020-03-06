//
// Created by usser on 2019/6/19.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include "TaskOutputStream.h"

#include <Interpreters/Connection/TaskOutputStream.h>
#include <IO/ConnectionTimeouts.h>

namespace DB {


    class TaskConnectionClient {

    public:
        TaskConnectionClient(std::string host_ , UInt16 port_ ):host(host_),port(port_),resolved_address(host_,port_){

            connected = false;
        }
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in ;
        std::shared_ptr<WriteBuffer> out ;
        std::shared_ptr<TaskOutputStream> out_stream ;
        bool connected ;
        ConnectionTimeouts timeouts;
        std::string host;
        UInt16 port;
        Poco::Net::SocketAddress resolved_address;
        void connect();
        void disconnect(){};
        //  DataReceiverInfo applyResource(std::string taskId);
        void sendTask(Task & task);
        void sendDone(std::string taskId);
        void checkStatus(std::string taskId);
        void sendHello(){};
        void receiveHello(){};
        TaskSource getExechangeSourceInfo(std::string taskId);
        std::string askReady();

    };


}