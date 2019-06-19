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
    std::map<std::string , UInt32> taskIdToPort;

public:
    void runImpl();


    void receivePackage();
    void receiveApplyRequest(); // start task exec and data receive thread , need to be thread safe
    bool receiveTask();
    void initTask();
    void execTask();
    void finishTask();


};
}

