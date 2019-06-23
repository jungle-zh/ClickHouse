//
// Created by usser on 2019/6/19.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include "TaskOutputStream.h"

namespace DB {


    class TaskConnectionClient {

    public:
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        std::shared_ptr<WriteBuffer> out;

        std::shared_ptr<TaskOutputStream> block_out;

        Executor applyResource(std::string TaskId);
        void sendTask(Task & task);


    };


}