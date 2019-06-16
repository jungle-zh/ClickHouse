//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/MergePlanNode.h>

namespace DB {


    void MergePlanNode::initDistribution(Distribution & distribution_){

        distribution = distribution_;

    }


}