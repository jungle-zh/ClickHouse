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
        buildStageTask(root);
        submitStage(root);                      // data receiver 个数 和 child stage 个数一样
    }

    void TaskScheduler::assignDataReciver(std::shared_ptr<Stage> root) {


        //std::vector<Partition> parts;
        if (root->scanDistribution && !root->exechangeDistribution) {  // stage with scanNode , data all come from table

            // no data receiver
            ScanPlanNode * scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());

            std::vector<std::shared_ptr<TaskConnectionClient>> taskClients = connectTaskReceiverForStageScanPart(root);

            assert(taskClients.size() ==  (size_t)root->getPartitionNum());
            for (int i = 0; i < root->getPartitionNum(); ++i) {
                //taskToConnection[root->getTaskId(i)] = taskClients[i];

                taskToConnection.insert({root->getTaskId(i),taskClients[i]});
            }
            // do not need for dataReceiver ;

        } else if (root->scanDistribution && root->exechangeDistribution) { // has scan table and exechange input

            ScanPlanNode *scanPlanNode = root->getScanNode();
            scanPlanNode->buildFullDistribution();
            root->setScanDistribution(scanPlanNode->getDistribution());

            assert(root->getExechangeDistribution()->equals(*(root->getScanDistribution())));
            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver = connectTaskReceiverForStageScanPart(root);// scan node and receiver node in the same server
            assert(taskReceiver.size() ==  (size_t)root->getPartitionNum());

            for (int i = 0; i < root->getPartitionNum(); ++i) {
                taskToConnection.insert({root->getTaskId(i),taskReceiver[i]});
            }
             applyDataReceiverForStageExechangePart(taskReceiver, root);


        } else if (root->exechangeDistribution && !root->isResultStage()) {

            std::vector<std::shared_ptr<TaskConnectionClient>> taskReceiver = connectTaskReceiverForStageExechangePart(
                    root);
            assert(taskReceiver.size() ==  (size_t)root->getPartitionNum());

            for (int i = 0; i < root->getPartitionNum(); ++i) {
                taskToConnection.insert({root->getTaskId(i),taskReceiver[i]});
                //taskToConnection[root->getTaskId(i)] = taskReceiver[i];
            }
            applyDataReceiverForStageExechangePart(taskReceiver, root);




        } else if(root->exechangeDistribution && root->isResultStage()){ // data all come from exechange node


            assert(root->getExechangeDistribution()->partitionNum == 1);
            std::map<int,ExechangePartition> & parts = root->getExechangeDistribution()->partitionInfo;
            std::map<int,DataReceiverInfo> & receiver = root->getExechangeDistribution()->receiverInfo;
            // receiver is local TCPHandler ;
            //std::map<int,DataReceiverInfo> & receiver = root->getExechangeDistribution()->receiverInfo;

            ExechangePartition exechangePartition;
            exechangePartition.exechangeType = DataExechangeType::tresult;


            for(auto child : root->getChildStages()){
                std::vector<std::string > childTasks =  child->getTaskIds() ;
                for(auto taskid : childTasks){
                    exechangePartition.childTaskIds.push_back(taskid);
                }
            }
            exechangePartition.mainTableChildStageId  = root->mainintTableChildStageId;
            exechangePartition.partitionId = 0 ;
            parts.insert({0,exechangePartition});

            DataReceiverInfo dataReceiverInfo ;
            dataReceiverInfo.ip = "127.0.0.1"; //   receiver task in local node
            dataReceiverInfo.dataPort = 5000;

            receiver.insert({0,dataReceiverInfo});


            root->getExechangeDistribution()->isPartitionTaskAssigned = true;



        } else {
            throw Exception("not possible distribution");
        }


        for (auto child : root->getChildStages()) {
            assignDataReciver(child);
        }

    }




    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::connectTaskReceiverForStageScanPart(std::shared_ptr<Stage> root) {

        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        std::vector<TaskReceiverInfo> receivers = createTaskExecutorByScanDistribution(root->getScanDistribution());
        assert(receivers.size() == size_t (root->getPartitionNum()));
        for (int i = 0; i < root->getPartitionNum(); ++i) {
            TaskReceiverInfo rec = receivers[i];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;
    }


    std::vector<std::shared_ptr<TaskConnectionClient>>
    TaskScheduler::connectTaskReceiverForStageExechangePart(std::shared_ptr<Stage> root) {

        std::vector<TaskReceiverInfo> receivers = createTaskExecutorByExechangeDistribution(root->getExechangeDistribution());
        std::vector<std::shared_ptr<TaskConnectionClient>> ret;
        assert(receivers.size() == size_t (root->getPartitionNum()));
        for (int i = 0; i < root->getPartitionNum(); ++i) {
            TaskReceiverInfo rec = receivers[i];
            std::shared_ptr<TaskConnectionClient> conn = createConnection(rec);
            ret.push_back(conn);
        }
        return ret;


    }

    std::vector<TaskReceiverInfo> TaskScheduler::createTaskExecutorByScanDistribution(DB::ScanDistribution *s) {
        std::vector<TaskReceiverInfo> ret ;
        for (int i = 0; i < s->partitionNum; ++i) {
            TaskReceiverInfo info("127.0.0.1" , 6000);
            ret.push_back(info);
        }

        return  ret ;
    }

    std::vector<TaskReceiverInfo> TaskScheduler::createTaskExecutorByExechangeDistribution(DB::ExechangeDistribution *e) {
        std::vector<TaskReceiverInfo> ret ;
        for (int i = 0; i < e->partitionNum; ++i) {
            TaskReceiverInfo info("127.0.0.1" , 6000);
            ret.push_back(info);
        }

        return  ret ;
    }


    void TaskScheduler::applyDataReceiverForStageExechangePart(
            std::vector<std::shared_ptr<TaskConnectionClient>> taskClients, std::shared_ptr<Stage> root) {

        ExechangeDistribution * exechangeDistribution = root->getExechangeDistribution();
        assert(exechangeDistribution != NULL);
        std::map<int,ExechangePartition> & parts = exechangeDistribution->partitionInfo;
        std::map<int,DataReceiverInfo> & receiver = exechangeDistribution->receiverInfo;

        for (int i = 0; i < root->getPartitionNum(); ++i) {

            DataReceiverInfo receiverInfo = taskClients[i]->applyResource(root->getTaskId(i)); // receiver for the  partition crrespond task
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

            //taskToConnection[root->getTaskId(i)] = conn;
        }
        //root->getExechangeDistribution()->setExechangePartitions(parts);


    }

    void TaskScheduler::buildStageTask(std::shared_ptr<DB::Stage> root) {

        root->buildTask(); //set executor in partition


        for (auto child : root->getChildStages()) {

            buildStageTask(child);
        }
    }

    void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> root) {

        //assert(root->getExechangeSender()->getDistribution()->isPartitionAssigned);
        //assert(root->getExechangeReceiver()->getDistribution()->isPartitionAssigned);


        for (auto task : root->getTasks()) {
            submitTask(*task.second);
        }

        for (auto child : root->getChildStages()) {

            submitStage(child);
        }

    }

    void TaskScheduler::startResultTask(Task & task){

        io.buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
        //task.setDataConnectionHandlerFactory(server->getDataConnectionHandlerFactory());
        task.initFinal();
        task.execute(io.buffer);

    }

    void TaskScheduler::submitTask(Task &task) {


        if(task.isScanTask()) {  //

           // task.getScanSource().partition.info;
            try {
                auto it = taskToConnection.find(task.getTaskId());
                assert(it != taskToConnection.end());
                it->second->sendTask(task);  // task  desc and source exechange  already build , remote task server and data server already start
            } catch (Exception e) {

            }


        } else if(task.isResultTask()) {

            pool.schedule(std::bind(&TaskScheduler::startResultTask, this,task));


        } else {

            try {
                auto it = taskToConnection.find(task.getTaskId());
                assert(it != taskToConnection.end());
                it->second->sendTask(task);  // task  desc and source exechange  already build , remote task server and data server already start
            } catch (Exception e) {

            }
        }




    }

    void TaskScheduler::checkTaskStatus(std::string taskId) {

        (void)taskId;
    }

    std::shared_ptr<TaskConnectionClient> TaskScheduler::createConnection(TaskReceiverInfo  & receiver ){

        auto connect  = std::make_shared<TaskConnectionClient>(receiver.ip,receiver.taskPort);
        connect->connect();
        return connect;
    }


}