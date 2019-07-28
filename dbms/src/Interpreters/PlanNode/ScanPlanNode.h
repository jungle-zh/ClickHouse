//
// Created by Administrator on 2019/3/31.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class ScanPlanNode : public PlanNode {

public:
    ScanPlanNode(std::string dbName_ ,std::string tableName_){
        dbName = dbName_;
        tableName = tableName_;
    }

public:
    void init();
    Block getHeader() override;
    void buildBaseDistribution(){
        std::vector<std::string> keys;
        if(tableName == "stu"){
            partitionNum = 2;
            keys.push_back("name");
        } else if(tableName == "score"){
            partitionNum = 3;
            keys.push_back("name");
        } else{
            partitionNum = 1 ;

        }
        distribution = std::make_shared<ScanDistribution>(keys ,partitionNum);
    }
    void buildFullDistribution(){

        std::map<int,ScanPartition> scanPartitions;

        for(int i=0;i< partitionNum ;++i){

            ScanPartition  scanPartition;

            scanPartition.partitionId = i;
            scanPartition.info.dbName = dbName;
            scanPartition.info.tableName = tableName;
            //scanPartition.info.host = hosts[i];

            scanPartitions.insert({i,scanPartition});
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

