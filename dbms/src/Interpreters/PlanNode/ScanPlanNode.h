//
// Created by Administrator on 2019/3/31.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class ScanPlanNode : public PlanNode {

    ScanPlanNode(std::string dbName_ ,std::string tableName_){
        dbName = dbName_;
        tableName = tableName_;
    }

public:
    void init();
    void buildBaseDistribution(){
        distribution = std::make_shared<ScanDistribution>(std::vector<std::string>() ,partitionNum);
    }
    void buildFullDistribution(){

        std::vector<ScanPartition> scanPartitions;

        for(int i=0;i< partitionNum ;++i){

            ScanPartition  scanPartition;

            scanPartition.partitionId = i;
            scanPartition.info.dbName = dbName;
            scanPartition.info.tableName = tableName;
            scanPartition.info.host = hosts[i];

            scanPartitions.push_back(scanPartition);
        }

        static_cast<ScanDistribution *>(distribution.get())->setScanPartitions(scanPartitions);
    }
private:
    int partitionNum;
    std::string dbName ;
    std::string tableName;
    std::vector<std::string> hosts;


};

}

