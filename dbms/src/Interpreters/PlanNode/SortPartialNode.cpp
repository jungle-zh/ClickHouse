//
// Created by Administrator on 2019/5/3.
//

#include <Interpreters/PlanNode/SortPartialNode.h>
#include <Interpreters/sortBlock.h>

namespace DB {

void SortPartialNode::init()    {

    std::shared_ptr<ExpressionActionsNode> expNode = std::make_shared<ExpressionActionsNode>(expressionActions);
    PlanNodePtr child =  getUnaryChild();
    expNode->setUnaryChild(child);
    this->setUnaryChild(expNode);
}

Block SortPartialNode::read() {


    Block block  = getUnaryChild()->read();
    sortBlock(block, description,limit);

    return block;

}



}
