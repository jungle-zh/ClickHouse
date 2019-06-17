//
// Created by jungle on 19-6-17.
//

#pragma once
namespace DB {


class TaskConnectionHandler : public Poco::Net::TCPServerConnection {


private:
    std::shared_ptr <ReadBuffer> in;
    std::shared_ptr <WriteBuffer> out;
    DataServer &server;
public:
    void runImpl();

    void receiveTask();


};
}

