//
// Created by Administrator on 2019/5/1.
//

#include <Interpreters/Exec/TaskSchedule.cpp>


namespace  DB {


std::shared_ptr<ExecutorProxy> TaskSchedule::chooseExecutor(Task & task){

    std::shared_ptr<ExecutorProxy> executor;

    if(task.type() == "top"){

    }else if( task.type() == "middle"){
        // task execute node  determine  the input exchange node position which is the output
        // exchange node position of the lower task


    } else {      // data partition node

    }

    task.setInputExchangeNodePos(executor);// lower task will get outPutExchangeNodePos;

}
void TaskSchedule::submitTask(Task & task) {


    std::shared_ptr<ExecutorProxy>  executor  = chooseExecutor(task);

    executor.submit(task);



}



}