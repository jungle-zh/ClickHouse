//
// Created by usser on 2019/6/17.
//

#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>

namespace DB {

void TaskScheduler::applyResourceAndSubmitStage(std::shared_ptr<Stage> root){

    assignExecutor(root);
    submitStage(root);
}

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
       parts = applyResourceForTask(root);
       //exechangeReceiver distributeKey and partitionNum is already set, only apply ip and port here ,
       // will exec task on this  ip and start data receiver on this port
       assert(parts.size() == root->getExechangeReceiver()->getDistribution()->partitions.size() && parts[0].executor.port != 0);
       root->getExechangeReceiver()->getDistribution()->setPartitions(parts);// parts with ip and port , so child stage know where to send data

   }
   for(auto child : root->getChildStage()){
       assignExecutor(child);
   }


}

std::vector<Partition> TaskScheduler::applyResourceForTask(std::shared_ptr<Stage> root){

    std::vector<Partition> parts = root->getExechangeReceiver()->getDistribution()->partitions;

    for(int i=0; i< parts.size() ;++i){
        TaskReceiver rec = receivers[++receiverIndex % totalReceiverNum];
        std::string taskId = root->getTaskId(i);
        std::shared_ptr<TaskConnectionClient> conn  = createConnection(rec);
        Executor e = conn->applyResource(taskId); // start task exec and data server thread and return their port
        parts[i].executor = e;
        taskToConnection[taskId] = conn;

    }
    return  parts;
}

void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> root) {

    assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
    assert(root->getExechangeReceiver()->getDistribution()->isPartitionAssigned);


    root->buildTask(); //set executor in partition

    for(auto  task : root->getTasks()){
        submitTask(*task);
    }

}


void TaskScheduler::submitTask(Task & task) {

    Executor e = task.getSource().partitions.executor;

    try {
        taskToConnection[e.taskId]->sendTask(task);
    }catch (Exception e){

    }


}
std::shared_ptr<TaskConnectionClient> TaskScheduler::createConnection(TaskReceiver receiver) {

}

void TaskScheduler::checkTaskStatus(std::string taskId){

}


}
