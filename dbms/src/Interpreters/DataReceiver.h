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
    DataReceiver(ExechangeTaskDataSource source){

        childTaskIds = source.partition.childTaskIds;
        exechangeType = source.partition.exechangeType;
        rightTableStageId = source.partition.rightTableChildStageId;
        mainTableStageIds = source.partition.mainTableChildStageId;
        port = source.receiver.dataPort;
        ip = source.receiver.ip;
        //inputHeader = inputHeader_;
        startToAccept();
    }

    void init();
    void startToAccept(); // receive and deserialize data

    //void addConnect(DataConnectionHandler * connect,int childStageId) { connections.insert({childStageId,connect});}

    std::shared_ptr<DataServer>  server;
    //std::shared_ptr<DataBuffer>  buffer; // read and fill in different thread ,buffer need to be thread safe
    // std::map<std::string,std::shared_ptr<DataBuffer>>  resultBuffer; // result senderId -> dataBuffer

    UInt32  port;
    std::string ip;
    //Blocks inputHeader ;
    bool  isResultReceiver;


    std::vector<std::string> childTaskIds; // stageId_taskId
    DataExechangeType  exechangeType;
    std::string rightTableStageId  ;
    std::vector<std::string> mainTableStageIds ;
    std::map<std::string,DataConnectionHandler * >  connections; // rstageId_taskId  receiver may have multi connection , for example ,join , aggMerge
    Task * task ;


    bool beloneTo(const std::string taskId, std::string stageId);
    bool beloneTo(const std::string taskId, std::vector<std::string> stageIds);
};


}

