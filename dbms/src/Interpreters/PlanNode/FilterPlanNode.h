//
// Created by Administrator on 2019/3/31.
//
#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class FilterPlanNode : public PlanNode {

public:
    FilterPlanNode(Block inputHeade_,std::shared_ptr<ExpressionActions> actions_,std::string filterColumn_){
        inputHeader = inputHeade_;
        actions = actions_;
        filterColumn = filterColumn_;
    }
private:
    Block inputHeader;
    std::shared_ptr<ExpressionActions> actions;
    std::string filterColumn;

    std::shared_ptr<ExecNode> createExecNode() override;


};

}
