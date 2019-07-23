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
    Names joinKeys;
    Block inputLeftHeader;
    Block inputRightHeader;
    std::string joinKind;
    std::string  strictness;

    JoinPlanNode(Names joinKey_, Block inputLeftHeader_ , Block inputRightHeader, std::string joinKind, std::string strictness);

    std::shared_ptr<ExecNode> createExecNode() override;


};

}
