//
// Created by usser on 2019/6/17.
//

#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>

namespace DB {

    void TaskScheduler::applyResourceAndSubmitStage(std::shared_ptr<Stage> root, DataReceiverInfo &resultReceiver) {

        assignDataReciver(root, resultReceiver);
        submitStage(root);
    }

    void TaskScheduler::assignDataReciver(std::shared_ptr<Stage> root, DataReceiverInfo &resultReceiver) {


        //std::vector<Partition> parts;
        if (root->isScanStage() && root->noChildStage()) {  // stage with scanNode , data all come from table

           ScanPlanNode * scanPlanNode =  root->getScanNode();
           scanPlanNode->buildFullDistribution();
           root->setScanDistribution(scanPlanNode->getDistribution());


        } else if (root->isScanStage() && !root->noChildStage()) {

            ScanPlanNode * scanPlanNode =  root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());

            assert(root->getExechangeDistribution()->equals(root->getScanDistribution()));
            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver =  applyTaskReceiverForStageScanPart(root);
            std::vector<ExechangePartition> parts = applyDataReceiverForStageExechangePart(taskReceiver, root);


        } else if (root->isResultStage()) {

            std::vector<ExechangePartition> parts = root->getExechangeDistribution()->exechangePartitions;
            assert(parts.size() == 1);
            parts[0].dataReceiverInfo = resultReceiver;
            root->getExechangeDistribution()->isPartitionTaskAssigned = true;

        } else { // data all come from exechange node

            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver = applyTaskReceiverForStageExechangePart(root);
            std::vector<ExechangePartition> parts = applyDataReceiverForStageExechangePart(taskReceiver, root);

        }
        for (auto child : root->getChildStage()) {
            assignDataReciver(child, resultReceiver);
        }


    }

    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::applyTaskReceiverForStageScanPart(std::shared_ptr<Stage> root) {

        std::vector<ScanPartition> parts = root->getScanDistribution()->scanPartitions;
        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        for (size_t i = 0; i < parts.size(); ++i) {
            TaskReceiverInfo rec = receivers[++receiverIndex % totalReceiverNum];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;
    }


    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::applyTaskReceiverForStageExechangePart(std::shared_ptr<Stage> root) {

        std::vector<ExechangePartition> parts = root->getExechangeDistribution()->exechangePartitions;
        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        for (size_t i = 0; i < parts.size(); ++i) {
            TaskReceiverInfo rec = receivers[++receiverIndex % totalReceiverNum];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;

    }

    std::vector<ExechangePartition> TaskScheduler::applyDataReceiverForStageExechangePart(
            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver, std::shared_ptr<Stage> root) {

        std::vector<ExechangePartition> parts = root->getExechangeDistribution()->exechangePartitions;

        for (size_t i = 0; i < parts.size(); ++i) {
            std::shared_ptr<TaskConnectionClient> conn = taskReceiver[i];
            DataReceiverInfo receiverInfo = conn->applyResource(
                    root->getTaskId(i)); // start data server thread and return their port
            parts[i].dataReceiverInfo = receiverInfo;
            parts[i].taskId = root->getTaskId(i);
            parts[i].partitionId = i;
            taskToConnection[receiverInfo.taskId] = conn;
        }
        root->getExechangeDistribution()->setExechangePartitions(parts);

    }

    /*
    std::vector<Partition> TaskScheduler::applyResourceForStage(std::shared_ptr<Stage> root) {

        std::vector<Partition> parts = root->getDistribution()->partitions;

        for (size_t i= 0; i < parts.size(); ++i) {
            TaskReceiverInfo rec = receivers[++receiverIndex % totalReceiverNum];
            std::string taskId = root->getTaskId(i);
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            DataReceiverInfo receiverInfo = conn->applyResource(taskId); // start data server thread and return their port
            parts[i].dataReceiverInfo = receiverInfo;
            taskToConnection[taskId] = conn;

        }
        return parts;
    }
    */

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

        DataReceiverInfo receiverInfo = task.getSource().partition.;

        try {
            taskToConnection[receiverInfo.taskId]->sendTask(task);
        } catch (Exception e) {

        }


    }

    std::shared_ptr<TaskConnectionClient> TaskScheduler::createConnection(TaskReceiver receiver) {

    }

    void TaskScheduler::checkTaskStatus(std::string taskId) {

    }


}
