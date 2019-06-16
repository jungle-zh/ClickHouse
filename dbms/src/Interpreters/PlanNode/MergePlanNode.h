//
// Created by Administrator on 2019/5/3.
//
#pragma once
#include <Interpreters/PlanNode/PlanNode.h>

#include <Interpreters/Aggregator.h>
#include <Core/Block.h>

namespace DB {



class MergePlanNode : public PlanNode {


private:

    Block inputHeader ;
    NamesAndTypesList aggregationKeys;
    NamesAndTypesList aggregateColumns;
    AggregateDescriptions  aggregateDescriptions ;

public:
    MergePlanNode(Block & inputHeader_ ,NamesAndTypesList & aggKeys_ , NamesAndTypesList & aggColumns_, AggregateDescriptions & desc_ )
    :inputHeader(inputHeader_),
    aggregationKeys(aggKeys_),
    aggregateColumns(aggColumns_),
    aggregateDescriptions(desc_){


    }

    void initDistribution(Distribution & distribution_) override;

};

}