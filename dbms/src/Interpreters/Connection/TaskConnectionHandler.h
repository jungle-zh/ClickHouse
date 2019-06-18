//
// Created by jungle on 19-6-17.
//

#pragma once

#include <IO/ReadBuffer.h>
#include <Poco/Net/TCPServerConnection.h>
#include "TaskInputStream.h"
#include "TaskServer.h"

namespace DB {


class TaskConnectionHandler : public Poco::Net::TCPServerConnection {


private:
    std::shared_ptr <TaskInputStream> in;
    std::shared_ptr <WriteBuffer> out;
    TaskServer &server;
    std::shared_ptr <Task> task;
public:
    void runImpl();



    bool receiveTask();
    void initTask();
    void execTask();
    void finishTask();


};
}

