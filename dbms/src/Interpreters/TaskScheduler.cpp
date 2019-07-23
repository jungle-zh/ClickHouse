//
// Created by usser on 2019/6/17.
//

#include <Server/TCPHandler.h>
#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>

namespace DB {

    void TaskScheduler::applyResourceAndSubmitStage(std::shared_ptr<Stage> root) {

        assignDataReciver(root);// stage 的 task receiver 个数 和 stage partitonNum 一样,每个 task 启动的
        submitStage(root);                      // data receiver 个数 和 child stage 个数一样
    }

    void TaskScheduler::assignDataReciver(std::shared_ptr<Stage> root) {


        //std::vector<Partition> parts;
        if (root->scanDistribution && !root->exechangeDistribution) {  // stage with scanNode , data all come from table

            ScanPlanNode *scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());


        } else if (root->scanDistribution && root->exechangeDistribution) { // has scan table and exechange input

            ScanPlanNode *scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());

            assert(root->getExechangeDistribution()->equals(*(root->getScanDistribution())));
            std::vector<std::shared_ptr<TaskConnectionClient>> taskClients = applyTaskReceiverForStageScanPart(root);
             applyDataReceiverForStageExechangePart(taskClients, root);


        } else if (root->exechangeDistribution && root->isResultStage()) {

            assert(root->getExechangeDistribution()->partitionNum == 1);
            std::map<int,ExechangePartition> & parts = root->getExechangeDistribution()->partitionInfo;

            // receiver is local TCPHandler ;
            //std::map<int,DataReceiverInfo> & receiver = root->getExechangeDistribution()->receiverInfo;

            parts[0].exechangeType = DataExechangeType::tresult;


            for(auto child : root->getChildStages()){
                std::vector<std::string > childTasks =  child->getTaskIds() ;
                parts[0].childTaskIds.insert(parts[0].childTaskIds.end(),childTasks.begin(),childTasks.end());
            }
            parts[0].partitionId = 0 ;

            root->getExechangeDistribution()->isPartitionTaskAssigned = true;

        } else if(root->exechangeDistribution){ // data all come from exechange node

            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver = applyTaskReceiverForStageExechangePart(
                    root);
             applyDataReceiverForStageExechangePart(taskReceiver, root);

        } else {
            throw Exception("not possible distribution");
        }


        for (auto child : root->getChildStages()) {
            assignDataReciver(child);
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

            DataReceiverInfo receiverInfo = conn->applyResource(root->getTaskId(i)); // receiver for the  partition crrespond task
            receiver.insert({i,receiverInfo});

            for(auto child : root->getChildStages()){
                std::vector<std::string > childTasks =  child->getTaskIds() ;
                parts[i].childTaskIds.insert(parts[i].childTaskIds.end(),childTasks.begin(),childTasks.end());
            }
            parts[i].partitionId = i;
            parts[i].exechangeType = root->sourceExechangeType;

            if(root->sourceExechangeType == DataExechangeType::ttwoshufflejoin ||
               root->sourceExechangeType == DataExechangeType::toneshufflejoin ||
               root->sourceExechangeType == DataExechangeType::tone2onejoin ) {
               parts[i].rightTableChildStageId = root->rightTableChildStageId;

            }
            parts[i].mainTableChildStageId = root->mainintTableChildStageId;

            taskToConnection[root->getTaskId(i)] = conn;
        }
        //root->getExechangeDistribution()->setExechangePartitions(parts);


    }


    void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> root) {

        //assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
        //assert(root->getExechangeReceiver()->getDistribution()->isPartitionAssigned);


        root->buildTask(); //set executor in partition

        for (auto task : root->getTasks()) {
            submitTask(*task.second);
        }

        for (auto child : root->getChildStages()) {

            submitStage(child);
        }

    }


    void TaskScheduler::submitTask(Task &task) {


        if(!task.getExecDest().isResult) {
            try {
                taskToConnection[task.getTaskId()]->sendTask(task);  // task  desc and source exechange  already build , remote task server and data server already start
            } catch (Exception e) {

            }
        } else {
             handler->startResultTask(task);
        }




    }

    void TaskScheduler::checkTaskStatus(std::string taskId) {


    }


}