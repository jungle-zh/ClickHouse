//
// Created by Administrator on 2019/3/31.
//

#pragma  once
#include <Interpreters/PlanNode/PlanNode.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionActions.h>

namespace DB {



class AggPlanNode : public PlanNode {

private:

    Block inputHeader ;
    ExpressionActionsPtr actions;
    NamesAndTypesList aggregationKeys;
    NamesAndTypesList aggregateColumns;
    AggregateDescriptions  aggregateDescriptions ;


public:
    AggPlanNode(Block & inputHeader_ ,ExpressionActionsPtr & expressionActions_,
                NamesAndTypesList & aggKeys_ , NamesAndTypesList & aggColumns_, AggregateDescriptions & desc_ )
    :inputHeader(inputHeader_),
     actions(expressionActions_),
     aggregationKeys(aggKeys_),
     aggregateColumns(aggColumns_),
     aggregateDescriptions(desc_){


    }


    void initDistribution() override ;






};


}
