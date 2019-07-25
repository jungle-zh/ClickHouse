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
        explicit TaskConnectionHandlerFactory( TaskServer * server_,bool secure_ = false)
                : server(server_), log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory")) {
        }


        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override ;




    };

}


