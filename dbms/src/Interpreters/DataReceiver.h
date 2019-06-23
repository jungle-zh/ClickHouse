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
    DataReceiver(DataSource source, Blocks & inputHeader_){
        taskId = source.partition.executor.taskId;
        port = source.partition.executor.dataPort;
        ip = source.partition.executor.ip;
        inputHeader = inputHeader_;
    }
    void startToReceive(); // receive and deserialize data
    Block read();             // call by logic thread
    Block read(std::string senderId);
    void fill(Block & block,std::string senderId); // call by io thread

    std::unique_ptr<DataServer>  server;
    std::shared_ptr<DataBuffer>  buffer; // read and fill in different thread ,buffer need to be thread safe
    // std::map<std::string,std::shared_ptr<DataBuffer>>  resultBuffer; // result senderId -> dataBuffer

    std::string taskId;
    UInt32  port;
    std::string ip;
    Blocks inputHeader ;
    bool  isResultReceiver;

};


}

