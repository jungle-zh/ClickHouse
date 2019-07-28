//
// Created by jungle on 19-6-30.
//

#pragma once


#include "ExecNode.h"
#include <Common/ConcurrentBoundedQueue.h>

namespace DB {

class TaskReceiverExecNode : public ExecNode {

public:
    TaskReceiverExecNode( std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer_){
        buffer = buffer_;
    }
    ~TaskReceiverExecNode(){};
    std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;
    void  readPrefix() override{};
    void  readSuffix() override{};
    Block read() override ;
    Block getHeader(bool isAnalyze) override  ;
    Block getInputHeader() override ;

};


}

