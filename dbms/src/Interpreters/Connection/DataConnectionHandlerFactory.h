//
// Created by jungle on 19-6-17.
//

#pragma  once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
//#include "DataConnectionHandler.h"

namespace DB {


    class DataServer;

    class DataConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:

        Poco::Logger * log;
        //DataServer & server;
        DataServer * server;
    public:
        //void setServer(DataServer * server_) { server  = server_;}
        DataConnectionHandlerFactory(){
            log  = &Logger::get("DataConnectionHandlerFactory");
        };
        void setDataServer(DataServer * server_){
            server  = server_;
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override ;

    };



}