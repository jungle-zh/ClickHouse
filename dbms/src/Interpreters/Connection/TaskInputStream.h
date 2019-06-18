//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Interpreters/Task.h>

namespace DB {

    class TaskInputStream  {


    public:
        void init();
        std::shared_ptr<Task> read();


    private:
        DataSource readTaskSource();
        DataDest readTaskDest();
        std::shared_ptr<ExecNode> readExecNode();

        std::shared_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
    };


}
