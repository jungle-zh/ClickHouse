//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/MergePlanNode.h>
#include <Interpreters/ExecNode/MergeExecNode.h>

namespace DB {



    std::shared_ptr<ExecNode> MergePlanNode::createExecNode() {
        auto mergeExecNode  = std::make_shared<MergeExecNode>(
                inputHeader,
                aggregationKeys,
                aggregateColumns,
                aggregateDescriptions,
                settings,
                context
        );

        return   mergeExecNode ;
    }


}