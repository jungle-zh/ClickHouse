//
// Created by jungle on 19-6-17.
//

#pragma  once

#include "TaskConnectionHandler.h"
#include <common/logger_useful.h>
namespace DB {


    class TaskConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:

        Poco::Logger *log;

    public:
        explicit TaskConnectionHandlerFactory( bool secure_ = false)
                :  log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory")) {
        }


        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override ;




    };

}


