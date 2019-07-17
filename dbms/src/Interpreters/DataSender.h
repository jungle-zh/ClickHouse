//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Client/Connection.h>
#include <Interpreters/Task.h>
#include <Interpreters/Connection/DataConnectionClient.h>


namespace DB {


class Block;

class DataSender {

public:

    DataSender(ExechangeTaskDataDest &  dest_ ){
         dest  = dest_;
    }

    ExechangeTaskDataDest dest;


    std::map<UInt32 , partitionInfo> partitions; // partitionid -> info
    std::map<UInt32 , std::shared_ptr<DataConnectionClient>> connections;   // partitionid -> connnections



    void send(Block & block);
    void tryConnect();

    std::map<UInt32,Block> repartitionByKey(Block & block);
    void addConnection();

    int  stageId ;

    Task * task ;

    //Poco::Logger * log;


};


}