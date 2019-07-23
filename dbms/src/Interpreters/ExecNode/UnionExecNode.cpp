//
// Created by jungle on 19-6-29.
//

#include "UnionExecNode.h"

namespace DB {

    Block UnionExecNode::read() {
        return  children->read(); // children is taskReceiverExecNode
    }

    Block UnionExecNode::getHeader() {
        return  children->getHeader();
    }
    Block UnionExecNode::getInputHeader() {
        return  children->getHeader();
    }


}