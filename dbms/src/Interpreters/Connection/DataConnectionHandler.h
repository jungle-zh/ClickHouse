//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "DataServer.h"

namespace DB {


class DataConnectionHandler : public  Poco::Net::TCPServerConnection {


private:
    std::shared_ptr<NativeBlockInputStream> in;
    std::shared_ptr<NativeBlockOutputStream> out;
    DataServer * server;

public:
    DataConnectionHandler( Poco::Net::StreamSocket & socket_,DataServer * server_)
    :Poco::Net::TCPServerConnection(socket_),
    server(server_) {

        in = std::make_shared<NativeBlockInputStream>(std::make_shared<ReadBufferFromPocoSocket>(socket()),1);
        out = std::make_shared<NativeBlockOutputStream>(std::make_shared<WriteBufferFromPocoSocket>(socket()),1);

    };
    void runImpl(); // receive data and fill

    bool receiveData();



};


}