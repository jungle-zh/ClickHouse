//
// Created by Administrator on 2019/4/3.
//

#include <Interpreters/PlanNode/ProjectPlanNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>

namespace DB {


    std::shared_ptr<ExecNode> ProjectPlanNode::createExecNode() {

        return std::make_shared<ProjectExecNode>(actions);

    }


}