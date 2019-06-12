//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/ExpressionActionsNode.h>


namespace DB {


Block ExpressionActionsNode::read() {

    Block  block = getUnaryChild()->read();

    actions.execute(block);


    return block;


}




}