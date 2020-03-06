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

class DataExechangeClient {

public:

    DataExechangeClient( std::map<std::string  , StageSource> &  source_ ,Task * task_ ,size_t partitionId_, Context * context_){
         //dest  = dest_;
         source = source_;
         task = task_;
        context  = context_;
        partitionId = partitionId_;
         log =  &Logger::get("DataExechangeClient");

    }

    //ExechangeTaskDataDest dest;
    std::map<std::string  , StageSource> source;
    std::set<std::string> finishedTask;
    std::set<std::string> finishedStage;

    std::vector<std::string> hashTableStages;
    std::map<std::string ,std::map<std::string,std::shared_ptr<DataConnectionClient>>> connections;
    // stageId  ->(taskid , connection)

    size_t partitionId ;

    //void send(Block & block);
    Block read(std::string stageId);
    void tryConnectAll();


    Task * task ;
    Context * context;
    Poco::Logger * log;



};


}