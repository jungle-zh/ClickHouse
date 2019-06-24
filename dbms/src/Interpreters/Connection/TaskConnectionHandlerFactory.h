//
// Created by jungle on 19-6-17.
//

#pragma  once

#include "TaskConnectionHandler.h"

namespace DB {


    class TaskConnectionHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

    private:

        Poco::Logger *log;

    public:
        explicit TaskConnectionHandlerFactory( bool secure_ = false)
                :  log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory")) {
        }

        Poco::Net::TCPServerConnection *createConnection(const Poco::Net::StreamSocket &socket) override {
            LOG_TRACE(log,
                      "TCP Request. "
                              << "Address: "
                              << socket.peerAddress().toString());

            //return new TCPHandler(server, socket);
            //return  new ExchangeNodeTcpHandler(node,socket);
            TaskConnectionHandler *handler = new TaskConnectionHandler(socket);

            //node.addHandler(std::shared_ptr<ExchangeNodeTcpHandler>(handler));

            return handler;
        }


    };

}


