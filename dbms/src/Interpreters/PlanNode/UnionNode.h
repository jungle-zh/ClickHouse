//
// Created by usser on 2019/6/12.
//

#pragma  once
#include <Interpreters/PlanNode/PlanNode.h>
namespace DB {


class UnionNode :public  PlanNode{

public:

    UnionNode();

    Block getHeader() override;



};





}


