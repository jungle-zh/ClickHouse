//
// Created by usser on 2019/6/12.
//

#pragma  once
#include <Interpreters/PlanNode/PlanNode.h>
namespace DB {


class UnionPlanNode :public  PlanNode{

public:

    std::shared_ptr<ExecNode>  createExecNode() override;
    UnionPlanNode(){};



};





}


