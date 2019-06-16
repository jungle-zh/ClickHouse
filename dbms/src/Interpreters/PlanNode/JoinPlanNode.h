//
// Created by Administrator on 2019/3/31.
//

#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/Join.h>
namespace DB {



class  JoinPlanNode : public PlanNode {

private:

public:


    JoinPlanNode();

    Names joinKeys;
    bool prepared;


};

}
