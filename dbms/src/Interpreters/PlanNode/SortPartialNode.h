//
// Created by usser on 2019/4/2.
//



#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Core/SortDescription.h>
#include <Interpreters/ExpressionActions.h>


namespace DB {

class  SortPartialNode  : public  PlanNode {

public:

    std::shared_ptr<ExpressionActions> expressionActions;
    SortDescription description;
    size_t limit ;
public:

    SortPartialNode(std::shared_ptr<ExpressionActions> expressionActions_ ,SortDescription & description_,size_t limit_) :
            expressionActions(expressionActions_),description(description_),limit(limit_){

    }



};


}