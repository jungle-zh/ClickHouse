//
// Created by usser on 2019/6/17.
//

#include "Stage.h"

namespace DB {



    void Stage::buildTask() {

        // only scan
        // only exechange
        // scan and exechange

        if(exechangeDistribution && ! scanDistribution) {


            std::map<int,std::vector<std::string>>  childStageToTask;
            for (size_t i = 0; i < childs.size(); ++i) {

                std::vector<std::string> inputTaskIds;
                if(childs[i]->getScanDistribution()){
                    for (int j = 0; j < childs[i]->getScanDistribution()->partitionNum; ++j) {
                        inputTaskIds.push_back(childs[i]->getTaskId(j));
                    }
                } else {
                    for (int j = 0; j < childs[i]->getExechangeDistribution()->partitionNum; ++j) {
                        inputTaskIds.push_back(childs[i]->getTaskId(j));
                    }
                }
                childStageToTask[childs[i]->stageId] = inputTaskIds;
            }

            for (int i = 0; i < getExechangeDistribution()->partitionNum; ++i) {

                auto task = std::make_shared<Task>() ;

                for(size_t j ;j< childs.size(); ++j){
                    ExechangeTaskDataSource source;
                    source.childStageId = childs[j]->stageId;
                    source.distributeKeys = exechangeDistribution->distributeKeys;
                    source.partition = getExechangeDistribution()->childStageToExechangePartitions[source.childStageId][i];
                    source.inputTaskIds = childStageToTask[childs[j]->stageId]; // one2one maybe only one input
                    task.addSource(source);

                }

                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitions = father->getExechangeDistribution()->childStageToExechangePartitions[stageId];   //father receiver is already set
                    dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                    task->setDest(dest);
                }else {
                    // no dest ,fill buffer and wait to be consumed

                }
            }

        } else if(!exechangeDistribution && scanDistribution){

            for (size_t i = 0; i < getScanDistribution()->scanPartitions.size(); ++i) {
                auto task = std::make_shared<Task>();

            }
        } else {


            std::map<int,std::vector<std::string>>  childStageToTask;
            for (size_t i = 0; i < childs.size(); ++i) {

                std::vector<std::string> inputTaskIds;
                if(childs[i]->getScanDistribution()){
                    for (int j = 0; j < childs[i]->getScanDistribution()->partitionNum; ++j) {
                        inputTaskIds.push_back(childs[i]->getTaskId(j));
                    }
                } else {
                    for (int j = 0; j < childs[i]->getExechangeDistribution()->partitionNum; ++j) {
                        inputTaskIds.push_back(childs[i]->getTaskId(j));
                    }
                }
                childStageToTask[childs[i]->stageId] = inputTaskIds;
            }

            assert(getScanDistribution()->equals(getExechangeDistribution()));

            std::map<int,ScanPartition> scanPartitions ;
            for (int i = 0; i < getScanDistribution()->partitionNum; ++i) {
                scanPartitions.insert({i,getScanDistribution()->scanPartitions[i]});
            }

            for (int i = 0; i < getExechangeDistribution()->partitionNum; ++i) {

                auto task = std::make_shared<Task>() ;
                for(size_t j ;j< childs.size(); ++j){
                    ExechangeTaskDataSource source;
                    source.childStageId = childs[j]->stageId;
                    source.distributeKeys = exechangeDistribution->distributeKeys;
                    source.partition = getExechangeDistribution()->childStageToExechangePartitions[source.childStageId][i];
                    source.inputTaskIds = childStageToTask[childs[j]->stageId]; // one2one maybe only one input
                    task.addSource(source);

                }


                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitions = father->getExechangeDistribution()->childStageToExechangePartitions[stageId];   //father receiver is already set
                    dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                    task->setDest(dest);
                }else {
                    // no dest ,fill buffer and wait to be consumed

                }

                ScanTaskDataSource source1;
                source1.distributeKeys  = getScanDistribution()->distributeKeys;
                source1.partition = scanPartitions[i]; // same partitionId;

                task->setScanSource(source1);


            }

        }

    }

}
