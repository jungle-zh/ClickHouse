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

            std::vector<std::string> inputTaskIds;
            for (size_t i = 0; i < childs.size(); ++i) {
                if(childs[i]->getScanDistribution()){
                    for (size_t j = 0; j < childs[i]->getScanDistribution()->scanPartitions.size(); ++j) {
                        inputTaskIds.push_back(childs[i]->getScanDistribution()->scanPartitions[j].taskId);
                    }
                } else {
                    for (size_t j = 0; j < childs[i]->getExechangeDistribution()->exechangePartitions.size(); ++j) {
                        inputTaskIds.push_back(childs[i]->getExechangeDistribution()->exechangePartitions[j].taskId);
                    }
                }

            }

            for (size_t i = 0; i < getExechangeDistribution()->exechangePartitions.size(); ++i) {

                auto task = std::make_shared<Task>() ;

                ExechangeTaskDataSource source;
                source.type = sourceExechangeType;
                source.distributeKeys = exechangeDistribution->distributeKeys;
                source.partition = getExechangeDistribution()->exechangePartitions[i];
                source.inputTaskIds = inputTaskIds; // one2one maybe only one input


                task->setExechangeSource(source);
                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitions = father->getExechangeDistribution()->exechangePartitions;   //father receiver is already set
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

            assert(getScanDistribution()->equals(getExechangeDistribution()));
            std::vector<std::string> inputTaskIds;
            for (size_t i = 0; i < childs.size(); ++i) {
                if(childs[i]->getScanDistribution()){
                    for (size_t j = 0; j < childs[i]->getScanDistribution()->scanPartitions.size(); ++j) {
                        inputTaskIds.push_back(childs[i]->getScanDistribution()->scanPartitions[j].taskId);
                    }
                } else {
                    for (size_t j = 0; j < childs[i]->getExechangeDistribution()->exechangePartitions.size(); ++j) {
                        inputTaskIds.push_back(childs[i]->getExechangeDistribution()->exechangePartitions[j].taskId);
                    }
                }
            }
            std::map<UInt32,ScanPartition> scanPartitions ;
            for (size_t i = 0; i < getScanDistribution()->scanPartitions.size(); ++i) {
                scanPartitions.insert({getScanDistribution()->scanPartitions[i].partitionId,getScanDistribution()->scanPartitions[i]});
            }

            for (size_t i = 0; i < getExechangeDistribution()->exechangePartitions.size(); ++i) {

                auto task = std::make_shared<Task>() ;
                ExechangeTaskDataSource source;
                source.type = sourceExechangeType;
                source.distributeKeys = exechangeDistribution->distributeKeys;
                source.partition = getExechangeDistribution()->exechangePartitions[i];
                source.inputTaskIds = inputTaskIds; // one2one maybe only one input
                task->setExechangeSource(source);
                if(!isResultStage()){
                    ExechangeTaskDataDest dest ;
                    dest.partitions = father->getExechangeDistribution()->exechangePartitions;   //father receiver is already set
                    dest.distributeKeys = father->getExechangeDistribution()->distributeKeys;
                    task->setDest(dest);
                }else {
                    // no dest ,fill buffer and wait to be consumed

                }

                ScanTaskDataSource source1;
                source1.distributeKeys  = getScanDistribution()->distributeKeys;
                source1.partition = scanPartitions[source.partition.partitionId]; // same partitionId;

                task->setScanSource(source1);


            }

        }

    }

}
