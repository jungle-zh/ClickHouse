//
// Created by jungle on 19-6-17.
//

#pragma once

#include <Poco/Net/TCPServerConnection.h>

namespace DB {


class DataConnectionHandler : public  Poco::Net::TCPServerConnection {


private:
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    DataServer & server;
public:
    void runImpl();

    void receiveData();



};


}