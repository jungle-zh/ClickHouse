//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include "DataServer.h"

namespace DB {


class DataConnectionHandler : public  Poco::Net::TCPServerConnection {


private:

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    std::shared_ptr<NativeBlockInputStream> block_in;
    std::shared_ptr<NativeBlockOutputStream> block_out;
    DataServer * server;
    std::string senderId ; // senderId is set in receive hello;

public:
    DataConnectionHandler(const Poco::Net::StreamSocket & socket_,DataServer * server_)
    :Poco::Net::TCPServerConnection(socket_),
    server(server_) {

        block_in = std::make_shared<NativeBlockInputStream>(std::make_shared<ReadBufferFromPocoSocket>(socket()),1);
        block_out = std::make_shared<NativeBlockOutputStream>(std::make_shared<WriteBufferFromPocoSocket>(socket()),1);

    };
    void runImpl(); // receive data and fill

    bool receiveData();



};


}