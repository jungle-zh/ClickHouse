//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/ExpressionActions.h>
#pragma  once

namespace DB {




class ExpressionActionsNode : public PlanNode {

public:
    Block read() override;

private:
    ExpressionActions actions;


};




}