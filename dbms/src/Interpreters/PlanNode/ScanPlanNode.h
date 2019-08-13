//
// Created by Administrator on 2019/3/31.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class ScanPlanNode : public PlanNode {

public:
    ScanPlanNode(std::string dbName_ ,std::string tableName_ ,std::set<std::string> required_column_, std::string query_, Context *context_){
        dbName = dbName_;
        tableName = tableName_;
        required_column = required_column_;
        query = query_;
        context  = context_;
    }

public:
    void init();
    Block getHeader() override;
    std::shared_ptr<ExecNode>  createExecNode() override;
    void buildBaseDistribution(){
        std::vector<std::string> keys;
        if(tableName == "stu"){
            partitionNum = 2;
            keys.push_back("name");
        } else if(tableName == "score"){
            partitionNum = 1;
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
    std::set<std::string> required_column ;
    std::string query;
    Context * context ;

};

}

