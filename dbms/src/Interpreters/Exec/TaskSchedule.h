//
// Created by Administrator on 2019/5/1.
//

#pragma  once

namespace DB {


class  TaskSchedule {

public:

    static void submitTask(Task & task);

    static std::shared_ptr<ExecutorProxy> chooseExecutor(Task & task);


    std::vector<ExecutorProxy>  executors;


};




}