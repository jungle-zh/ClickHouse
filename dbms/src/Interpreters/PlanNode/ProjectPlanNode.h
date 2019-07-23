//
// Created by Administrator on 2019/3/31.
//

#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/ExpressionActions.h>


namespace DB {

class  ProjectPlanNode  : public  PlanNode {

    ProjectPlanNode(Block header_ ,std::shared_ptr<ExpressionActions> actions_){
        header = header_;
        actions = actions_;
    }
private:
    Block header;
    std::shared_ptr<ExpressionActions> actions;
public:

    std::shared_ptr<ExecNode> createExecNode() override;





};


}

