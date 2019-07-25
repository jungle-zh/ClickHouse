//
// Created by jungle on 19-6-17.
//

#include "TaskConnectionHandlerFactory.h"

#include <Interpreters/Connection/TaskConnectionHandler.h>

namespace DB {

 Poco::Net::TCPServerConnection *TaskConnectionHandlerFactory::createConnection(const Poco::Net::StreamSocket &socket)  {
        LOG_TRACE(log,
          "TCP Request. "
                  << "Address: "
                  << socket.peerAddress().toString());

        TaskConnectionHandler * handler = new TaskConnectionHandler(socket,server);

        return handler;
    }
}
