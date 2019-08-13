//
// Created by jungle on 19-6-17.
//

#pragma  once

#include "TaskConnectionHandler.h"
#include <common/logger_useful.h>
#include <Poco/Net/TCPServerConnectionFactory.h>

namespace DB {


    class TaskConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:
        TaskServer * server;
        Poco::Logger *log;


    public:
        TaskConnectionHandlerFactory(){
            log  = &Logger::get("TaskConnectionHandlerFactory");
        };

        void setTaskServer(TaskServer * server_) {
            server  = server_;
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override ;




    };

}


