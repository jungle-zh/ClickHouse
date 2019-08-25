//
// Created by jungle on 19-6-30.
//

#pragma once


#include "ExecNode.h"
#include <Common/ConcurrentBoundedQueue.h>

namespace DB {
class Task ;
class DataClientExecNode : public ExecNode {

public:
    DataClientExecNode(std::shared_ptr<DataExechangeClient> client_,Task * task_ ,std::vector<std::string> stageIds_){
        client = client_;
        task = task_;
        log = &Logger::get("TaskReceiverExecNode");
        stageIds  = stageIds_;
        bufferMaxSize = 100000;
        buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
    }
    ~DataClientExecNode(){};
    std::shared_ptr<DataExechangeClient> client ;
    Task *  task ;
    Poco::Logger *log  ;
    std::vector<std::string> stageIds;
    std::set<std::string>  finishedStage;
    //bool  allConsumed;
    std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;
    size_t bufferMaxSize ;
    Block header;

    void  readPrefix(std::shared_ptr<DataExechangeClient>) override{};
    void  readSuffix() override{};
    Block read() override ;
    Block readFromBuffer();
    void  readFromRemote();
    Block getHeader(bool isAnalyze) override  ;
    Block getInputHeader() override ;
    std::string getName() override { return  "taskReceiverExecNode";}

};


}

