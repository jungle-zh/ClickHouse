//
// Created by jungle on 19-6-30.
//

#pragma once


#include "ExecNode.h"
#include <Common/ConcurrentBoundedQueue.h>

namespace DB {
class Task ;
class TaskReceiverExecNode : public ExecNode {

public:
    TaskReceiverExecNode( std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer_,Task * task_){
        buffer = buffer_;
        task = task_;
        log = &Logger::get("TaskReceiverExecNode");
    }
    ~TaskReceiverExecNode(){};
    std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;
    Task *  task ;
    Poco::Logger *log  ;
    //bool  allConsumed;
    void  readPrefix() override{};
    void  readSuffix() override{};
    Block read() override ;
    Block getHeader(bool isAnalyze) override  ;
    Block getInputHeader() override ;
    std::string getName() override { return  "taskReceiverExecNode";}

};


}

