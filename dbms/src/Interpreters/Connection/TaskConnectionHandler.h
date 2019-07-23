//
// Created by jungle on 19-6-17.
//

#pragma once

#include <IO/ReadBuffer.h>
#include <Poco/Net/TCPServerConnection.h>
#include <common/ThreadPool.h>
#include "TaskInputStream.h"
#include "TaskServer.h"

namespace DB {


class TaskConnectionHandler : public Poco::Net::TCPServerConnection {


private:
    std::shared_ptr <TaskInputStream> in_stream;
    std::shared_ptr<ReadBuffer> in ;
    std::shared_ptr <WriteBuffer> out;
    TaskServer * server;
    std::shared_ptr <Task> task;
    std::map<std::string , UInt32> taskIdToPort;

    //std::shared_ptr<std::thread> taskRunner;
    ThreadPool pool{1};

    std::exception_ptr exception;

    Context * connection_context;
public:
    void run();
    void runImpl();
    void runTask();


    void receivePackage();
    void receiveApplyRequest(); // start task exec and data receive thread , need to be thread safe
    void receiveTask();


    TaskConnectionHandler(const Poco::Net::StreamSocket & socket ):
    Poco::Net::TCPServerConnection(socket){

    }
};
}

