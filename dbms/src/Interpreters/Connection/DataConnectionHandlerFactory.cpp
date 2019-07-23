//
// Created by jungle on 19-6-17.
//

#include "DataConnectionHandlerFactory.h"
#include <Interpreters/Connection/DataConnectionHandler.h>

namespace DB {

    Poco::Net::TCPServerConnection * DataConnectionHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket)
{
    //LOG_TRACE(log,"TCP Request." << " Address:" << socket.peerAddress().toString());

    //return new TCPHandler(server, socket);
    //return  new ExchangeNodeTcpHandler(node,socket);
    return(Poco::Net::TCPServerConnection *) new DataConnectionHandler(socket);

   // return handler;
}


}