//
// Created by usser on 2019/6/17.
//

#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>

namespace DB {

void TaskScheduler::assignExecutor(std::shared_ptr<Stage> root){


    if(root->isResultStage()){
        //dest not set yet
    }else {
        assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
    }

    std::vector<Partition>  parts;
   if(root->isScanStage() && root->noChildStage()){  // stage with scanNode
     //  parts = applyScanExecutorForStage(root); // ip is know ,no receiver but table
   } else {
       parts = applyExecutorsForStage(root);
       //exechangeReceiver distributeKey and partitionNum is already set, only apply ip and port here ,
       // will exec task on this  ip and start data receiver on this port
       assert(parts.size() == root->getExechangeReceiver()->getDistribution()->partitions.size());
       root->getExechangeReceiver()->getDistribution()->setPartitions(parts);// parts with ip and port , so child stage know where to send data

   }


}

void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> root) {

    assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
    assert(root->getExechangeReceiver()->getDistribution()->isPartitionAssigned);


    //if stage receiver distribution equal with sender , may use one to one sender
    //else use shuffle sender
    //or to simply ,all use shuffer way

    //just need to care the stage exechangeSender distribution,
    //submit stage to the exechangeReceiver partition 


}

}
