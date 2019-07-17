//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Core/Block.h>
#include <Interpreters/Task.h>
#include <Interpreters/Connection/DataServer.h>

namespace DB {


class  DataConnectionHandler;
class DataReceiver {

public:
    DataReceiver(ExechangeTaskDataSource source, Blocks & inputHeader_){

        childTaskIds = source.partition.childStageIds;
        exechangeType = source.partition.exechangeType;
        rightTableStageId = source.partition.rightTableStageId;
        port = source.receiver.dataPort;
        ip = source.receiver.ip;
        inputHeader = inputHeader_;
        startToAccept();
    }

    void init();
    //bool getStartToReceive();
    //void setStartToReceive(bool startToReceive);
    void startToAccept(); // receive and deserialize data
    Block read();             // call by logic thread
    Block read(std::string senderId);
    void fill(Block & block,std::string senderId); // call by io thread
    //void addConnect(DataConnectionHandler * connect,int childStageId) { connections.insert({childStageId,connect});}

    std::unique_ptr<DataServer>  server;
    //std::shared_ptr<DataBuffer>  buffer; // read and fill in different thread ,buffer need to be thread safe
    // std::map<std::string,std::shared_ptr<DataBuffer>>  resultBuffer; // result senderId -> dataBuffer

    UInt32  port;
    std::string ip;
    Blocks inputHeader ;
    bool  isResultReceiver;
    //bool   startToReceive = false;

    std::vector<std::string> childTaskIds; // stageId_taskId
    DataExechangeType  exechangeType;
    int rightTableStageId  = -1 ;
    int mainTableStageId = -1;
    std::map<std::string,DataConnectionHandler * >  connections; // rstageId_taskId  receiver may have multi connection , for example ,join , aggMerge
    Task * task ;


};


}

