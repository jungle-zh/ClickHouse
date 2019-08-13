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
    std::string taskId ;
    std::string taskType ;
    std::map<std::string , UInt32> taskIdToPort;

    //std::shared_ptr<std::thread> taskRunner;
    ThreadPool pool{1};

    std::exception_ptr exception;

    Context * connection_context;
    Poco::Logger  * log;
public:
    void run() override;
    void runImpl();
    void runTask();


    void receivePackage();
    void receiveApplyRequest(); // start task exec and data receive thread , need to be thread safe
    void receiveTask();

    void receiveTaskDone();
    void receiveCheckTask();
    TaskConnectionHandler(const Poco::Net::StreamSocket & socket ,TaskServer * server_ ):
    Poco::Net::TCPServerConnection(socket),server(server_){
        log  = &Logger::get("TaskConnectionHandler");
    }
};
}

