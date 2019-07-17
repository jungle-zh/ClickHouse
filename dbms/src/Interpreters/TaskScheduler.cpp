//
// Created by usser on 2019/6/17.
//

#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>

namespace DB {

    void TaskScheduler::applyResourceAndSubmitStage(std::shared_ptr<Stage> root, DataReceiverInfo &resultReceiver) {

        assignDataReciver(root, resultReceiver);// stage 的 task receiver 个数 和 stage partitonNum 一样,每个 task 启动的
        submitStage(root);                      // data receiver 个数 和 child stage 个数一样
    }

    void TaskScheduler::assignDataReciver(std::shared_ptr<Stage> root, DataReceiverInfo &resultReceiver) {


        //std::vector<Partition> parts;
        if (root->scanDistribution && !root->exechangeDistribution) {  // stage with scanNode , data all come from table

            ScanPlanNode *scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());


        } else if (root->scanDistribution && root->exechangeDistribution) { // has scan table and exechange input

            ScanPlanNode *scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());

            assert(root->getExechangeDistribution()->equals(root->getScanDistribution()));
            std::vector<std::shared_ptr<TaskConnectionClient>> taskClients = applyTaskReceiverForStageScanPart(root);
             applyDataReceiverForStageExechangePart(taskClients, root);


        } else if (root->isResultStage()) {

            std::vector<ExechangePartition> parts = root->getExechangeDistribution()->exechangePartitions;
            assert(parts.size() == 1);
            parts[0].dataReceiverInfo = resultReceiver;
            root->getExechangeDistribution()->isPartitionTaskAssigned = true;

        } else if(root->exechangeDistribution){ // data all come from exechange node

            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver = applyTaskReceiverForStageExechangePart(
                    root);
             applyDataReceiverForStageExechangePart(taskReceiver, root);

        } else {
            throw Exception("not possible distribution");
        }


        for (auto child : root->getChildStage()) {
            assignDataReciver(child, resultReceiver);
        }

    }




    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::applyTaskReceiverForStageScanPart(std::shared_ptr<Stage> root) {

        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        std::vector<TaskReceiverInfo> receivers = createTaskExecutorByScanDistribution(root->getScanDistribution());
        for (int i = 0; i < root->getPartitionNum(); ++i) {
            TaskReceiverInfo rec = receivers[++receiverIndex % totalReceiverNum];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;
    }


    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::applyTaskReceiverForStageExechangePart(std::shared_ptr<Stage> root) {

        std::vector<TaskReceiverInfo> receivers = createTaskExecutorByExechangeDistribution(root->getExechangeDistribution());
        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        for (int i = 0; i < root->getExechangeDistribution()->partitionNum; ++i) {
            TaskReceiverInfo rec = receivers[++receiverIndex % totalReceiverNum];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;


    }

    std::vector<TaskReceiverInfo> TaskScheduler::createTaskExecutorByScanDistribution(DB::ScanDistribution *s) {

    }

    std::vector<TaskReceiverInfo> TaskScheduler::createTaskExecutorByExechangeDistribution(DB::ExechangeDistribution *e) {

    }


    void TaskScheduler::applyDataReceiverForStageExechangePart(
            std::vector<std::shared_ptr<TaskConnectionClient>> taskClients, std::shared_ptr<Stage> root) {

        std::map<int,ExechangePartition> & parts = root->getExechangeDistribution()->partitionInfo;
        std::map<int,DataReceiverInfo> & receiver = root->getExechangeDistribution()->receiverInfo;

        for (int i = 0; i < root->getPartitionNum(); ++i) {

            std::shared_ptr<TaskConnectionClient> conn = taskClients[i];
            DataReceiverInfo receiverInfo = conn->applyResource(); // receiver for the  partition crrespond task
            receiver.insert({i,receiverInfo});

            for(auto child : root->getChildStages()){
                parts[i].childStageIds.push_back(child->stageId);
            }
            parts[i].partitionId = i;
            parts[i].exechangeType = root->sourceExechangeType;

            if(root->sourceExechangeType == DataExechangeType::ttwoshufflejoin){
                assert(root->getChildStages().size() == 2);
                parts[i].rightTableStageId = root->getChildStages()[1]->stageId;
            } else if (root->sourceExechangeType == DataExechangeType::toneshufflejoin ||
               root->sourceExechangeType == DataExechangeType::tone2onejoin){
                assert(root->getChildStages().size() == 1);
                parts[i].rightTableStageId =root->getChildStages()[0]->stageId;
            }

            taskToConnection[root->getTaskId(i)] = conn;
        }
        //root->getExechangeDistribution()->setExechangePartitions(parts);


    }


    void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> root) {

        //assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
        //assert(root->getExechangeReceiver()->getDistribution()->isPartitionAssigned);


        root->buildTask(); //set executor in partition

        for (auto task : root->getTasks()) {
            submitTask(*task);
        }

        for (auto child : root->getChildStage()) {

            submitStage(child);
        }

    }


    void TaskScheduler::submitTask(Task &task) {

        try {
            taskToConnection[task.getTaskId()]->sendTask(task);  // task  desc and source exechange  already build , remote task server and data server already start
        } catch (Exception e) {

        }


    }

    void TaskScheduler::checkTaskStatus(std::string taskId) {


    }


}