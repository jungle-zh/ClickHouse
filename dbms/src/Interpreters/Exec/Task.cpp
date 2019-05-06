//
// Created by Administrator on 2019/5/1.
//
#include <Interpreters/Exec/Task.h>
#include <Interpreters/PlanNode/JoinNode.h>
#include <IO/VarInt.h>

namespace DB {


void Task::serialize(WriteBuffer & buffer) {


     // serialize info :
     //input exchange node info

     //output exchange nodes info

     //planNodes info

     ExchangeNode::serialize(buffer);

     writeVarUInt(outputs.size(),buffer);

     for (int i = 0; i < outputs.size() ; ++i) {
          outputs[i]->serialize(buffer)
     }

     writeVarUInt(nodes.size(),buffer);

     for (int j = 0; j <  nodes.size() ; ++j) {

          nodes[j]->serialize(buffer);
     }


}


void Task::deserialize(ReadBuffer & buffer) {


     input =  ExchangeNode::deserialize(buffer);
     size_t  outputExchangeNum  ;
     readVarUInt(outputExchangeNum,buffer);

     for(int i=0;i<outputExchangeNum;++i){


     }

}
void Task::executeTask() {


     /*
     if(nodes[0]->getName() == "shuffleJoin"){

          JoinNode  node = typeid_cast<JoinNode &>(*(nodes[0]));
          node.prepareHashJoinTable(input);

     }
     */
     while (1){

          /*
          Block block = pullFromInput(); //if join , then it pull from the scan table
          if(block.size() == 0)
               break;


          for(int i =0;i < nodes.begin(); ++i){

               nodes[i]->execute(block);
          }

          pushToOutPuts(block);
           */

          //last node  child need to set to exchange input node
          //nodes[nodes.size()-1]->clearChild();
          //nodes[nodes.size()-1]->addChild(input);

          Block block = nodes[0]->read(); //call child's read and deal

          if(block.size() == 0 )
               break;
          pushToOutPuts(block);

     }

}

void Task::getDataExchangeProxyByOutputInfo(Context & context){

}

}