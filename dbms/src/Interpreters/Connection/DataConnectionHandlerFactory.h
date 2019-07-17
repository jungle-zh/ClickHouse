//
// Created by jungle on 19-6-17.
//

#pragma  once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Logger.h>
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
        explicit DataConnectionHandlerFactory(DataServer * server_,bool secure_ = false):
                 log(&Poco::Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "DataHandlerFactory"))
        {
            server = server_;
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override ;



    };



}