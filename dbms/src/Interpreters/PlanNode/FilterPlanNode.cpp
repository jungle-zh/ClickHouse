//
// Created by jungle on 19-7-24.
//
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/FilterExecNode.h>
#include "FilterPlanNode.h"

namespace DB {


    std::shared_ptr<ExecNode> FilterPlanNode::createExecNode() {

        return  std::make_shared<FilterExecNode>(
                filterColumn,
                actions,
                inputHeader
                );


    }
}
