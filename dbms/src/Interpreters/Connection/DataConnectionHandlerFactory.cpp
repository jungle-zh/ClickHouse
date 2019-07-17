//
// Created by jungle on 19-6-17.
//

#include "DataConnectionHandlerFactory.h"

namespace DB {

    Poco::Net::TCPServerConnection * DataConnectionHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket) override
{
    //LOG_TRACE(log,"TCP Request." << " Address:" << socket.peerAddress().toString());

    //return new TCPHandler(server, socket);
    //return  new ExchangeNodeTcpHandler(node,socket);
    DataConnectionHandler * handler = new DataConnectionHandler(socket,server);

    //node.addHandler(std::shared_ptr<ExchangeNodeTcpHandler>(handler));

    return  handler;
}


}