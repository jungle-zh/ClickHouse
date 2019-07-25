//
// Created by jungle on 19-7-26.
//
#include <Interpreters/ExecNode/ExecNode.h>
#include "PlanNode.h"
namespace DB {

    Block PlanNode::getHeader() {
        std::shared_ptr<ExecNode> execNode =  createExecNode();
        execNode->readPrefix();
        return  execNode->getHeader();
    }


}

