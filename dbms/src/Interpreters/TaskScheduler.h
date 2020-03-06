//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/Stage.h>
#include <Client/Connection.h>
#include <Interpreters/Connection/TaskConnectionClient.h>
#include <common/ThreadPool.h>

namespace DB {

class TCPHandler ;
class ExechangePartition;
class TaskScheduler {


public:
    TaskScheduler(){
        log = &Logger::get("TaskScheduler");
    }

    void schedule(std::shared_ptr<DB::Stage> current);
    void assignDistributionToChildStage(std::shared_ptr<Stage> root);
    void fillStageSource(std::shared_ptr<DB::Stage> current);
    void getStagesWithNoDependence(std::shared_ptr<DB::Stage> current,std::vector<std::shared_ptr<Stage>> & res);
    //void applyResourceAndSubmitStage(std::shared_ptr<Stage> root);
    //void assignDataReciver(std::shared_ptr<Stage> root);
    std::vector<ExechangePartition> applyResourceForStage(std::shared_ptr<Stage> stage);

    std::vector<std::shared_ptr<TaskConnectionClient>> connectTaskReceiverForStageScanPart(std::shared_ptr<Stage> root);
    std::vector<std::shared_ptr<TaskConnectionClient>> connectTaskReceiverForStageExechangePart(std::shared_ptr<Stage> root);

    void applyDataReceiverForStageExechangePart(
            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver, std::shared_ptr<Stage> root);
    BlockIO getBlockIO() { return  io;}
private:

    void buildStageTask(std::shared_ptr<Stage> root);
    void submitStage(std::shared_ptr<Stage> root);
    void submitTask(Task & task);
    void startResultTask(Task * task);

    void checkTaskStatus(std::string taskId); // check task and close connection if task is finished  in seperate thread



    std::shared_ptr<TaskConnectionClient> createConnection(TaskReceiverInfo   receiver );
    //std::vector<TaskReceiverInfo> receivers;

    std::map<UInt32 ,TaskReceiverInfo> createTaskExecutorByScanDistribution(ScanSource & s);
    std::map<UInt32,TaskReceiverInfo>createTaskExecutorByDistribution(Distribution & d);
    void sendTaskDone(std::string taskId);    // if task status is finished ,send task done package and close connection;


    std::map<std::string , std::map<std::string,TaskSource>> submittedTaskInfo;
    std::map<std::string , std::shared_ptr<TaskConnectionClient>> taskToConnection;

    int receiverIndex;
    int totalReceiverNum;
    int resultReceiverPort;
    ThreadPool pool{1};
    BlockIO io;
    //TCPHandler * handler;

    Logger * log;





    void debugStage(std::shared_ptr<Stage> cur, std::stringstream  & ss, int blankNum) ;
};


}


