//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/Stage.h>
#include <Client/Connection.h>
#include <Interpreters/Connection/TaskConnectionClient.h>
namespace DB {


class TaskScheduler {


public:
    void applyResourceAndSubmitStage(std::shared_ptr<Stage> root,DataReceiverInfo & resultReceiver);
    void assignDataReciver(std::shared_ptr<Stage> root,DataReceiverInfo & resultReceiver);
    std::vector<ExechangePartition> applyResourceForStage(std::shared_ptr<Stage> stage);
    void submitStage(std::shared_ptr<Stage> root);
    void submitTask(Task & task);

    void checkTaskStatus(std::string taskId); // check task and close connection if task is finished  in seperate thread


    std::shared_ptr<TaskConnectionClient> createConnection(TaskReceiverInfo receiver );
    std::vector<TaskReceiverInfo> receivers;
    std::map<std::string , std::shared_ptr<TaskConnectionClient>> taskToConnection;

    int receiverIndex;
    int totalReceiverNum;



};


}


