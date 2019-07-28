//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/JoinPlanNode.h>
#include <Interpreters/PlanNode/ExechangeNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>

namespace DB {


    std::shared_ptr<ExecNode> JoinPlanNode::createExecNode() {
        return  std::make_shared<JoinExecNode>(
                    joinKeys,
                    inputLeftHeader,
                    inputRightHeader,
                    joinKind,
                    strictness
                );
    }
}
