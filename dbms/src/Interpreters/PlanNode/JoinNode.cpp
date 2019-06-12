//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/JoinNode.h>
#include <Interpreters/PlanNode/ExchangeNode.h>

namespace DB {

Block JoinNode::read() {

   if(!prepared){
       prepareRightTable();
       prepared = true;
   }

   Block block =  childs[0]->read();

   join->joinBlock(block);

    return  block;

}
void JoinNode::prepareRightTable() {

    const ExchangeNode * child =    typeid_cast<const ExchangeNode * >(childs[0].get())

    while(Block block = child->readRightTable()){

        join->insertFromBlock(block);
    }

}

}
