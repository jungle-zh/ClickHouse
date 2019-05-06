//
// Created by usser on 2019/4/2.
//



#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Core/SortDescription.h>


namespace DB {

class  SortPartialNode  : public  PlanNode {

private:

    std::shared_ptr<ExpressionActions> expressionActions;
    SortDescription description;
    size_t limit ;
public:

    SortPartialNode(SortDescription && description_,std::shared_ptr<ExpressionActions> expressionActions_ ,size_t limit_) :
    description(description_),expressionActions(expressionActions_),limit(limit_){

    }

    void init () override;

    Block read() override ;


};


}