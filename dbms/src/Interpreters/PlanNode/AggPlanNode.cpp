//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/AggPlanNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/PlanNode/ExpressionActionsNode.h>
namespace DB {


    void AggPlanNode::initDistribution() { // same with child

        distribution.distributionHash = getUnaryChild()->getDistribution().distributionHash;

        distribution.executorId = getUnaryChild()->getDistribution().executorId;


    }


}
