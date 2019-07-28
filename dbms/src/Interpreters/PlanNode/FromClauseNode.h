//
// Created by usser on 2019/6/12.
//
#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>

namespace DB {


class FromClauseNode : public PlanNode {

public:
    FromClauseNode(){};
    ~FromClauseNode(){

    }
    Block getHeader() override{
        return  getChild(0)->getHeader();
    }
    std::string getName() override{
        return  "fromClauseNode";
    }

};



}