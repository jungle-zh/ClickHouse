//
// Created by Administrator on 2019/5/3.
//
#pragma once
#include <Interpreters/PlanNode/PlanNode.h>

#include <Interpreters/Aggregator.h>
#include <Core/Block.h>

namespace DB {


struct   AggMergeNodeInfo {

    Block header; //before expressionAction


    Aggregator::Params params;

};
class AggMergeNode : public PlanNode {


private:
    Aggregator::Params params;
    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
    Aggregator  aggregator ;
public:
    AggMergeNode( Aggregator::Params && params_)
    :params(params_),   aggregator(params_){
    }
    Block read() override ;



};

}