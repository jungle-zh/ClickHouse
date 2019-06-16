//
// Created by Administrator on 2019/5/4.
//

#pragma  once

#include <Poco/Net/TCPServerConnection.h>
#include <Interpreters/PlanNode/ExchangeNodeTcpHandler.h>
namespace DB {


class ExchangeNodeTcpHandlerFactory : public Poco::Net::TCPServerConnectionFactory {

private:
    ExechangeNode & node;
    Poco::Logger * log;

public:
    explicit ExchangeNodeTcpHandlerFactory(ExechangeNode & node_, bool secure_ = false)
    : node(node_)
    , log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        LOG_TRACE(log,
                  "TCP Request. "
                          << "Address: "
                          << socket.peerAddress().toString());

        //return new TCPHandler(server, socket);
        //return  new ExchangeNodeTcpHandler(node,socket);
        ExchangeNodeTcpHandler * handler = new ExchangeNodeTcpHandler(node,socket);

        node.addHandler(std::shared_ptr<ExchangeNodeTcpHandler>(handler));

        return  handler;
    }


};
}