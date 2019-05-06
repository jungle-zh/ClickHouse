//
// Created by Administrator on 2019/5/1.
//


#include <Interpreters/Exec/Stage.h>
#include <Interpreters/Exec/TaskSchedule.h>

namespace DB {

void Stage::whereToSubmitTasks () {


}

void Stage::buildResultTask() {

    std::shared_ptr<Task> task = std::make_shared<Task>(topNode,"result",1,taskCnt++,getTaskId()); // not set outout yet , no taskid , task is scheduled to all node
    tasks.push_back(task);
}
void Stage::buildScanTask(){

    std::shared_ptr<Task> task = std::make_shared<Task>(topNode,"Scan"); // not set outout yet , no taskid , task is scheduled to all node
    tasks.push_back(task);
}
std::string  Stage::getTaskId(){
    return  std::to_string(stageId) + "_" + taskCnt;
}
std::shared_ptr<PlanNode>  bottomNode() {

}
void Stage::buildTask() {


        if(bottomNode()->type() == "AggMerge"){
            std::shared_ptr<Task> task = std::make_shared<Task>(topNode,"AggMerge",1,taskCnt++,getTaskId()); // not set outout yet
            tasks.push_back(task);
            break;
        } else if(bottomNode()->type() == "SortMerge"){
            std::shared_ptr<Task> task = std::make_shared<Task>(topNode,"SortMerge",1,taskCnt++, getTaskId()); // not set outout yet
            tasks.push_back(task);
            break;
        } else if(bottomNode()->type() == "ShuffleJoin"){
            JoinNode *  node = typeid_cast<JoinNode * >(bottomNode.get());
            int part =  node->joinParition() ;
            for(int i =0 ;i < part ; ++i) {
                std::shared_ptr<Task> task = std::make_shared<Task>(topNode,"ShuffleJoin",part,taskCnt++, getTaskId()); // not set outout yet
                tasks.push_back(task);
            }
        }


}
void Stage::submitTasks() { // input exchange and output exchange node are all definite

    if(type() == "top"){

    }else if( type() == "middle"){
        // task execute node  determine  the input exchange node position which is the output
        // exchange node position of the lower task


    } else {      // data partition node

    }


    for(int i=0;i<tasks.size() ;++i){


        TaskSchedule::submitTask(tasks[i]);

    }


}

}