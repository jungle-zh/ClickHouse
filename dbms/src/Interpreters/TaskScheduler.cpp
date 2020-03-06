//
// Created by usser on 2019/6/17.
//

#include <Server/TCPHandler.h>
#include <Interpreters/TaskScheduler.h>
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>

namespace DB {



    void TaskScheduler::assignDistributionToChildStage(std::shared_ptr<Stage> root) {

        for(auto childId : root->getChildStageIds()){
           auto child = root->getChildStage(childId);
           child->fatherDistribution = root->distribution;

        }
        for(auto childId : root->getChildStageIds()){
            auto child = root->getChildStage(childId);
            assignDistributionToChildStage(child);
        }
    }

    std::map<UInt32,TaskReceiverInfo>TaskScheduler::createTaskExecutorByScanDistribution(DB::ScanSource &s) {
        std::map<UInt32,TaskReceiverInfo> ret ;
        for (size_t i = 0; i < s.partitionIds.size(); ++i) {
            TaskReceiverInfo info("127.0.0.1" , 6000);
            ret.insert({i,info});
        }

        return  ret ;
    }
    std::map<UInt32,TaskReceiverInfo> TaskScheduler::createTaskExecutorByDistribution(Distribution & d) {
        std::map<UInt32,TaskReceiverInfo> ret ;
        for (size_t i = 0; i < d.parititionIds.size(); ++i) {
            TaskReceiverInfo info("127.0.0.1" , 6000);
            ret.insert({i,info});
        }

        return  ret ;
    }


    std::shared_ptr<TaskConnectionClient> TaskScheduler::createConnection(TaskReceiverInfo   receiver ){

        auto connect  = std::make_shared<TaskConnectionClient>(receiver.ip,receiver.taskPort);
        connect->connect();
        return connect;
    }
    void TaskScheduler::fillStageSource(std::shared_ptr<DB::Stage> current) {

        std::vector<std::string> childIds = current->getChildStageIds();

        for (auto childId : childIds) {
            auto it = submittedTaskInfo.find(childId);
            if (it == submittedTaskInfo.end()) {
                throw Exception("child stage not submit yet ");
            } else {
                StageSource source;
                source.taskSources = it->second;
                for (size_t i = 0; i < current->distribution->parititionIds.size(); ++i) {
                    source.newPartitionIds.push_back(current->distribution->parititionIds[i]);
                }
                source.newDistributeKeys = current->distribution->distributeKeys;

                current->stageSource.insert({childId, source});
            }

        }

    }


    void TaskScheduler::submitStage(std::shared_ptr<DB::Stage> current) {

        if(current->hasExechange)
            fillStageSource(current);
        if(current->hasScan){
            assert(current->scanSource.partition.size() > 0); //already set
        }

        std::map<UInt32 ,TaskReceiverInfo> taskExecutorInfo ;
        if(current->isResultStage_){

        }else if(current->hasScan){
            taskExecutorInfo =  createTaskExecutorByScanDistribution(current->scanSource);
        }
        else if(current->hasExechange){
            taskExecutorInfo =  createTaskExecutorByDistribution(*current->distribution);
        }

        current->buildTask();// need to add task partitionId and taskId


        std::map<std::string,TaskSource> currentStageSource;
        if(current->isResultStage_){ // final stage

            std::vector<std::string> taskIds = current->getTaskIds() ;
            assert(taskIds.size() == 1);
            size_t partitionId = 0;
            auto task = current->getTask(partitionId);

            pool.schedule(std::bind(&TaskScheduler::startResultTask, this,task.get()));
            pool.wait();
            TaskSource source;
            currentStageSource.insert({task->getTaskId(),source});

        }else if(current->hasScan){
            size_t  taskNum = current->scanSource.partitionIds.size();
            assert(taskExecutorInfo.size() == taskNum);
            for(UInt32 partitionId : current->scanSource.partitionIds){
                auto taskClient =   createConnection(taskExecutorInfo[partitionId]);
                Task * task = current->getTasks()[partitionId].get();
                taskClient->sendTask(*task);

                std::string isSourceReady  = "";
                while(isSourceReady != "ready"){
                    isSourceReady  = taskClient->askReady();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                TaskSource taskSource = taskClient->getExechangeSourceInfo(task->getTaskId());
                currentStageSource.insert({task->getTaskId(),taskSource});
                taskToConnection.insert({task->getTaskId(),taskClient});
            }

        }else if(current->hasExechange){
            size_t  taskNum = current->distribution->parititionIds.size();
            assert(taskExecutorInfo.size() == taskNum);
            for(size_t partitionId : current->distribution->parititionIds){
                auto taskClient =   createConnection(taskExecutorInfo[partitionId]);
                Task * task = current->getTasks()[partitionId].get();
                taskClient->sendTask(*task);

                std::string isSourceReady  = "";
                while(isSourceReady != "ready"){
                    isSourceReady  = taskClient->askReady();
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                TaskSource taskSource = taskClient->getExechangeSourceInfo(task->getTaskId());
                currentStageSource.insert({task->getTaskId(),taskSource});
                taskToConnection.insert({task->getTaskId(),taskClient});
            }

        }
        if(currentStageSource.size())
            submittedTaskInfo.insert({current->stageId,currentStageSource});

    }

    void TaskScheduler::getStagesWithNoDependence(std::shared_ptr<DB::Stage> current,std::vector<std::shared_ptr<Stage>> & res){

        if(submittedTaskInfo.find(current->stageId) != submittedTaskInfo.end())
            return;

        bool  hasDepdence = false;
        for(auto  childId : current->getChildStageIds()){
            if(submittedTaskInfo.find(childId) == submittedTaskInfo.end()){
                hasDepdence = true;
                break;
            }
        }
        if(!hasDepdence){
            res.push_back(current);
        }

        for(auto  childId : current->getChildStageIds()){
            auto child =   current->getChildStage(childId);
            getStagesWithNoDependence(child,res);
        }

    }

    void TaskScheduler::schedule(std::shared_ptr<DB::Stage> root){


        assignDistributionToChildStage(root);
        while(true){
            std::vector<std::shared_ptr<Stage>> readyStage;
            getStagesWithNoDependence(root,readyStage);
            if(readyStage.size() == 0){
                LOG_DEBUG(log,"all stage finished schedule");
                break;
            }
            for(auto ready : readyStage){
                submitStage(ready);
            }
        }

        std::stringstream ss;
        debugStage(root,ss,5);
        LOG_DEBUG(log,ss.str());
    }

    void TaskScheduler::startResultTask(Task * task){

        io.buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
        //task.setDataConnectionHandlerFactory(server->getDataConnectionHandlerFactory());
        task->initFinal();
        task->execute(io.buffer);

    }
    /*

    void TaskScheduler::applyResourceAndSubmitStage(std::shared_ptr<Stage> root) {

        assignDataReciver(root);// stage 的 task receiver 个数 和 stage partitonNum 一样,每个 task 启动的
        buildStageTask(root);
        std::stringstream ss ;
        ss << "\n";
        debugStage(root,ss,0);
        LOG_DEBUG(log,ss.str());
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


     */

    void TaskScheduler::debugStage(std::shared_ptr<Stage> cur, std::stringstream  & ss, int blankNum) {

        cur->debugString(ss,blankNum);
        ss << "\n";
        for(auto childId : cur->getChildStageIds()){
            auto child = cur->getChildStage(childId);
            debugStage(child,ss, blankNum + 5);
        }


    }

}