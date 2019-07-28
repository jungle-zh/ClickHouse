//
// Created by jungle on 19-6-17.
//

#include "DataServer.h"
#include <Interpreters/DataReceiver.h>
#include <Interpreters/Connection/DataConnectionHandlerFactory.h>


namespace DB {


    DataServer::DataServer(int port):
            portNum(port){
        connectionFactory = std::make_unique<DataConnectionHandlerFactory>(this);
        server = std::make_unique<Poco::Net::TCPServer>(connectionFactory.get(),portNum);

        //connectionFactory->setServer(this);

    }



}