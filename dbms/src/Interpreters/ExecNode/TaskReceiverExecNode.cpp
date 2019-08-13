//
// Created by jungle on 19-6-30.
//


#include <common/logger_useful.h>
#include "TaskReceiverExecNode.h"
#include <Interpreters/Task.h>


namespace DB {



    Block TaskReceiverExecNode::read() {


        Block block;
        buffer->pop(block); // will block if buffer empty
        if(!block){
            LOG_DEBUG(log,"task: "+ task->getTaskId() + " received all child task data");
        } else {
            LOG_DEBUG(log,"task: "+ task->getTaskId() + " received block row size:"  << block.rows() );
        }

        return  block;
    }

    Block TaskReceiverExecNode::getHeader(bool isAnalyze) {
        (void) isAnalyze;
        return  buffer->front();
    }

    Block TaskReceiverExecNode::getInputHeader() {
        return  buffer->front();
    }





}