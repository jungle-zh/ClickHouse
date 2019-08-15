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

    std::string hashTable = "";

    JoinPlanNode(Names joinKey_, Block inputLeftHeader_ , Block inputRightHeader_, std::string joinKind_, std::string strictness_){
        joinKeys= joinKey_;
        inputLeftHeader = inputLeftHeader_;
        inputRightHeader = inputRightHeader_;
        joinKind = joinKind_;
        strictness = strictness_;
    }

    Block getHeader() override;
    std::shared_ptr<ExecNode> createExecNode() override;


    void setHashTable(std::string);
};

}
