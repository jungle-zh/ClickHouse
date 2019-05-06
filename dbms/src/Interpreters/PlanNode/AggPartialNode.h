//
// Created by Administrator on 2019/3/31.
//

#pragma  once
#include <Interpreters/PlanNode/PlanNode.h>

#include <Interpreters/Aggregator.h>

namespace DB {



class AggPartialNode : public PlanNode {

private:
    Aggregator::Params params;

public:
    AggPartialNode(std::shared_ptr<ExpressionActions> aggExpressionActions_,Aggregator::Params && params_)
    :expressionActions(aggExpressionActions_),
    params(params_),
    aggregator(params_){

    }
    AggPartialNode ();

    void  beforeAggExpression();
    Block read() override ;

    void init();
    bool  executed;
    std::shared_ptr<ExpressionActions> expressionActions;
    Aggregator  aggregator ;
    std::unique_ptr<IBlockInputStream> impl;


};


}
