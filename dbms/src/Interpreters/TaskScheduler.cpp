//
// Created by usser on 2019/6/17.
//

#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>

namespace DB {

void TaskScheduler::submitStage(std::shared_ptr<Stage> root){



    std::vector<executorId> executors;
   if(root->Distribution()->executors.size()){  // stage with scanNode
       executors = root->Distribution()->executors;
   } else {
       executors = applyExecutorsForStage(root); //need set the distribution info in stage exechange receiver

   }

    for(size_t i = 0; i < executors.size(); ++i){
        WriteBuffer buffer;
        root->serializeTaskHeader(buffer);
        root->serializeTaskSourceInfo(executors[i],buffer);
        root->serializeTaskExecNode(buffer);
        root->seralizeTaskDescInfo(executors[i],buffer);
        executorConnection[executors[i]].sendTask(buffer);
    }



}

}
