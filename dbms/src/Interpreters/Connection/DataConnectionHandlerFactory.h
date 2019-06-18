//
// Created by jungle on 19-6-17.
//

#pragma  once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Logger.h>
#include "DataConnectionHandler.h"

namespace DB {



    class DataConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:

        Poco::Logger * log;
        //DataServer & server;
        DataServer * server;
    public:
        void setServer(DataServer * server_) { server  = server_;}
        explicit DataConnectionHandlerFactory(bool secure_ = false):
                 log(&Poco::Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        {
        }

        Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
        {
            LOG_TRACE(log,"TCP Request." << " Address:" << socket.peerAddress().toString());

            //return new TCPHandler(server, socket);
            //return  new ExchangeNodeTcpHandler(node,socket);
            DataConnectionHandler * handler = new DataConnectionHandler(socket,server);

            //node.addHandler(std::shared_ptr<ExchangeNodeTcpHandler>(handler));

            return  handler;
        }


    };



}