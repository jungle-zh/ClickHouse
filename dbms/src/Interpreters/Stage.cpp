//
// Created by usser on 2019/6/17.
//

#include <Interpreters/ExecNode/AggExecNode.h>
#include <Interpreters/PlanNode/ResultPlanNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/PlanNode/AggPlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>
#include <Interpreters/PlanNode/FilterPlanNode.h>
#include <Interpreters/ExecNode/FilterExecNode.h>
#include <Interpreters/PlanNode/JoinPlanNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/PlanNode/MergePlanNode.h>
#include <Interpreters/ExecNode/MergeExecNode.h>
#include <Interpreters/PlanNode/ProjectPlanNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include <Interpreters/ExecNode/ScanExecNode.h>
#include <Interpreters/PlanNode/UnionPlanNode.h>
#include <Interpreters/ExecNode/UnionExecNode.h>
#include "Stage.h"

namespace DB {


    std::vector<std::string > Stage::getTaskIds(){

        auto dis = exechangeDistribution == NULL ?  scanDistribution : exechangeDistribution;
        std::vector<std::string > ret ;
        for(int i=0;i< dis->partitionNum ;++i){
            ret.push_back( getTaskId(i));
        }
        return  ret ;

    }
    int  Stage::getPartitionNum() {

        if(scanDistribution){
            return  scanDistribution->partitionNum;
        } else {
            return  exechangeDistribution->partitionNum;
        }

    }



    void Stage::buildTaskExecNode(){

        for(auto p : planNodes){

            auto execNode = p->createExecNode();
            execNodes.push_back(execNode);
        }

    }
    void Stage::buildTask() {

        buildTaskExecNode();
        buildTaskSourceAndDest();



    }
    ScanPlanNode* Stage::getScanNode() {
        return static_cast<ScanPlanNode * >(planNodes[planNodes.size()-1].get());
    }

    void Stage::buildTaskSourceAndDest() {

        // only scan
        // only exechange
        // scan and exechange

        if(exechangeDistribution && ! scanDistribution) {


            std::map<std::string,std::vector<std::string>>  childStageToTask; // child stage -> child tasks
            for (size_t i = 0; i < childs.size(); ++i) {

                std::vector<std::string> inputTaskIds;
                for (int j = 0; j < childs[i]->getPartitionNum(); ++j) {
                    inputTaskIds.push_back(childs[i]->getTaskId(j));
                }
                childStageToTask[childs[i]->stageId] = inputTaskIds;
            }

            for (int i = 0; i < getExechangeDistribution()->partitionNum; ++i) {

                auto task = std::make_shared<Task>() ;


                ExechangeTaskDataSource source;
                source.distributeKeys = getExechangeDistribution()->distributeKeys;
                for(size_t j=0 ;j< childs.size(); ++j){

                    for(std::string taskId : childStageToTask[childs[j]->stageId]){
                        source.inputTaskIds.push_back(taskId);   // one2one maybe only one input
                    }

                }
                source.partition = getExechangeDistribution()->partitionInfo[i];
                source.receiver = getExechangeDistribution()->receiverInfo[i]; //  maybe from multi child task

                task->setExechangeSource(source);

                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitionInfo = father->getExechangeDistribution()->partitionInfo;   //father receiver is already set
                    dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                    dest.receiverInfo =  father->getExechangeDistribution()->receiverInfo; // paritionId -> receiver
                    task->setExechangeDest(dest);
                }else {
                    // no dest ,fill buffer and wait to be consumed
                    ExechangeTaskDataDest dest ;
                    dest.isResult = true;
                    task->setResultTask();
                    task->setExechangeDest(dest);

                }
                task->setExecNodes(execNodes);
                tasks.insert({i,task});
            }

        } else if(!exechangeDistribution && scanDistribution){

            assert(!isResultStage());

            std::map<int,ScanPartition> scanPartitions ;
            for (int i = 0; i < getScanDistribution()->partitionNum; ++i) {
                scanPartitions.insert({i,getScanDistribution()->scanPartitions[i]});
            }


            for (int i = 0; i < getPartitionNum(); ++i) {
                auto task = std::make_shared<Task>();
                ExechangeTaskDataDest dest ;
                dest.partitionInfo = father->getExechangeDistribution()->partitionInfo;   //father receiver is already set
                dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                dest.receiverInfo =  father->getExechangeDistribution()->receiverInfo;
                task->setExechangeDest(dest);

                ScanTaskDataSource source1;
                source1.distributeKeys  = getScanDistribution()->distributeKeys;
                source1.partition = scanPartitions[i]; // same partitionId;

                task->setScanSource(source1);


                task->setExecNodes(execNodes);
                task->setScanTask();
                tasks.insert({i,task});
            }

        } else { // have scan and exechange


            std::map<std::string,std::vector<std::string>>  childStageToTask;
            for (size_t i = 0; i < childs.size(); ++i) {

                std::vector<std::string> inputTaskIds;
                for (int j = 0; j < childs[i]->getPartitionNum(); ++j) {
                    inputTaskIds.push_back(childs[i]->getTaskId(j));
                }
                childStageToTask[childs[i]->stageId] = inputTaskIds;
            }

            assert(getScanDistribution()->equals(*getExechangeDistribution()));

            std::map<int,ScanPartition> scanPartitions ;
            for (int i = 0; i < getScanDistribution()->partitionNum; ++i) {
                scanPartitions.insert({i,getScanDistribution()->scanPartitions[i]});
            }

            for (int i = 0; i < getExechangeDistribution()->partitionNum; ++i) {

                auto task = std::make_shared<Task>() ;
                ExechangeTaskDataSource source;
                source.distributeKeys = getExechangeDistribution()->distributeKeys;

                for(size_t j ;j< childs.size(); ++j){
                    for(std::string taskId : childStageToTask[childs[j]->stageId]){
                        source.inputTaskIds.push_back(taskId);   // one2one maybe only one input
                    }
                }
                source.partition = getExechangeDistribution()->partitionInfo[i];
                source.receiver = getExechangeDistribution()->receiverInfo[i];   // each task has only one receiver
                task->setExechangeSource(source);

                ScanTaskDataSource source1;
                source1.distributeKeys  = getScanDistribution()->distributeKeys;
                source1.partition = scanPartitions[i]; // same partitionId;

                task->setScanSource(source1);

                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitionInfo = father->getExechangeDistribution()->partitionInfo;   //father receiver is already set
                    dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                    dest.receiverInfo =  father->getExechangeDistribution()->receiverInfo;    // each task has multi sender
                    task->setExechangeDest(dest);
                }else {
                    // no dest ,fill buffer and wait to be consumed

                    ExechangeTaskDataDest dest ;
                    dest.isResult = true;
                    task->setResultTask();
                    task->setExechangeDest(dest);
                }
                task->setExecNodes(execNodes);
                task->setScanTask();
                tasks.insert({i,task});
            }
        }
    }

}
