//
// Created by usser on 2019/6/19.
//

#pragma once

namespace DB {


    class TaskConnectionClient {

    public:
        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        std::shared_ptr<WriteBuffer> out;

        //std::shared_ptr<TaskOutputStream> block_out = std::make_shared<TaskOutputStream>(*in, server_revision);

        Executor applyResource(std::string TaskId);
        void sendTask(Task & task);


    };


}