//
// Created by Administrator on 2019/3/31.
//

#pragma  once
#include <Interpreters/PlanNode/PlanNode.h>

#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionActions.h>


namespace DB {

class ExecNode ;
class AggPlanNode : public PlanNode {

private:

    Block inputHeader ;
    ExpressionActionsPtr actions;
    NamesAndTypesList aggregationKeys;
    NamesAndTypesList aggregateColumns;
    AggregateDescriptions  aggregateDescriptions ;
    Context * context ;
    bool final  = false;

public:
    AggPlanNode(Block & inputHeader_ ,ExpressionActionsPtr & expressionActions_,
                NamesAndTypesList & aggKeys_ , NamesAndTypesList & aggColumns_, AggregateDescriptions & desc_ ,Context * context_)
    :inputHeader(inputHeader_),
     actions(expressionActions_),
     aggregationKeys(aggKeys_),
     aggregateColumns(aggColumns_),
     aggregateDescriptions(desc_),
     context(context_){


    }

    //Block getHeader() override ;

    std::shared_ptr<ExecNode> createExecNode() override;

    //void initDistribution() override ;
    Block  getHeader() override;

    void setFinal(bool final_){
        final = final_;
    }

};


}
