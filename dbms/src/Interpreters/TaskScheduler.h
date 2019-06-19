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
    void applyResourceAndSubmitStage(std::shared_ptr<Stage> root);
    void assignExecutor(std::shared_ptr<Stage> root);
    std::vector<Partition> applyResourceForTask(std::shared_ptr<Stage> stage);
    void submitStage(std::shared_ptr<Stage> root);
    void submitTask(Task & task);

    void checkTaskStatus(std::string taskId); // check task and close connection if task is finished  in seperate thread


    std::shared_ptr<TaskConnectionClient> createConnection(TaskReceiver receiver );
    std::vector<TaskReceiver> receivers;
    std::map<std::string , std::shared_ptr<TaskConnectionClient>> taskToConnection;

    int receiverIndex;
    int totalReceiverNum;



};


}


