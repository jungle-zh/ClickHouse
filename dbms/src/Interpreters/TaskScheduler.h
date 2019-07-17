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

    std::vector<std::shared_ptr<TaskConnectionClient>> applyTaskReceiverForStageScanPart(std::shared_ptr<Stage> root);
    std::vector<std::shared_ptr<TaskConnectionClient>> applyTaskReceiverForStageExechangePart(std::shared_ptr<Stage> root);

    void applyDataReceiverForStageExechangePart(
            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver, std::shared_ptr<Stage> root);
private:

    void submitStage(std::shared_ptr<Stage> root);
    void submitTask(Task & task);

    void checkTaskStatus(std::string taskId); // check task and close connection if task is finished  in seperate thread



    std::shared_ptr<TaskConnectionClient> createConnection(TaskReceiverInfo receiver );
    //std::vector<TaskReceiverInfo> receivers;

    std::vector<TaskReceiverInfo> createTaskExecutorByScanDistribution(ScanDistribution * s);
    std::vector<TaskReceiverInfo> createTaskExecutorByExechangeDistribution(ExechangeDistribution * e);
    void sendTaskDone(std::string taskId);    // if task status is finished ,send task done package and close connection;

    std::map<std::string , std::shared_ptr<TaskConnectionClient>> taskToConnection;

    int receiverIndex;
    int totalReceiverNum;
    int resultReceiverPort;



};


}


