//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/AggPlanNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/PlanNode/ExpressionActionsNode.h>
#include <Interpreters/ExecNode/AggExecNode.h>

namespace DB {



    std::shared_ptr<ExecNode> AggPlanNode::createExecNode() {
        auto aggExecNode  = std::make_shared<AggExecNode>(
                inputHeader,
                aggregationKeys,
                aggregateColumns,
                aggregateDescriptions,
                actions,
                settings,
                context
                );

        return   aggExecNode ;
    }



}
