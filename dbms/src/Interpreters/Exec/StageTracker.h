//
// Created by Administrator on 2019/5/6.
//

#pragma  once

class StageTacker {

public:
    void submitStage(Stage & stage);

    void submitTask(Task  & task);



    std::shared_ptr<TaskSchedule> schedule ;
};



