//
// Created by usser on 2019/6/19.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include "TaskOutputStream.h"

#include <Interpreters/Connection/TaskOutputStream.h>

namespace DB {


    class TaskConnectionClient {

    public:
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in ;
        TaskOutputStream out_stream ;

        DataReceiverInfo applyResource();
        void sendTask(Task & task);
        void sendDone(std::string taskId);
        void checkStatus(std::string taskId);


    };


}