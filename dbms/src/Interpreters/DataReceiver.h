//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Core/Block.h>
#include <Interpreters/Task.h>
#include <Interpreters/Connection/DataServer.h>

namespace DB {



class DataReceiver {

public:
    DataReceiver(ExechangeTaskDataSource source, Blocks & inputHeader_){
        inputTaskId = source.partition.inputTaskId;
        port = source.partition.dataReceiverInfo.dataPort;
        ip = source.partition.dataReceiverInfo.ip;
        inputHeader = inputHeader_;
    }

    bool getStartToReceive();
    void setStartToReceive(bool startToReceive);
    void startToAccept(); // receive and deserialize data
    Block read();             // call by logic thread
    Block read(std::string senderId);
    void fill(Block & block,std::string senderId); // call by io thread

    std::unique_ptr<DataServer>  server;
    std::shared_ptr<DataBuffer>  buffer; // read and fill in different thread ,buffer need to be thread safe
    // std::map<std::string,std::shared_ptr<DataBuffer>>  resultBuffer; // result senderId -> dataBuffer

    std::string inputTaskId;
    UInt32  port;
    std::string ip;
    Blocks inputHeader ;
    bool  isResultReceiver;
    bool   startToReceive = false;

};


}

