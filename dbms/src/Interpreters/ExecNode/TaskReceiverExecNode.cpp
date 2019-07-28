//
// Created by jungle on 19-6-30.
//


#include "TaskReceiverExecNode.h"


namespace DB {



    Block TaskReceiverExecNode::read() {

        Block block;
        buffer->pop(block); // will block if buffer empty
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