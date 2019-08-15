//
// Created by usser on 2019/6/12.
//

#include <Interpreters/PlanNode/UnionPlanNode.h>
#include <Interpreters/ExecNode/UnionExecNode.h>

namespace DB  {

    std::shared_ptr<ExecNode> UnionPlanNode::createExecNode() {
        return std::make_shared<UnionExecNode>(header);
    }
}