//
// Created by jungle on 19-6-18.
//
#pragma  once

#include <vector>
#include <string>
#include <memory>
#include <Core/Types.h>
#include <map>

namespace DB {


enum DataExechangeType {

   tone2onejoin,
   toneshufflejoin,
   ttwoshufflejoin,
   taggmerge,
   tunion,
   tresult
};


struct ServerNode {
    std::string ip;
    UInt32 port;
};

struct TaskReceiverInfo {

    TaskReceiverInfo(std::string ip_,UInt32 port_){

        ip = ip_;
        taskPort = port_;
    }
    std::string ip; // is static
    UInt32 taskPort;

};
struct DataReceiverInfo {
    std::string ip;  //dynamic allocate 
    UInt32 dataPort;
};
struct ExechangePartition{
    UInt32 partitionId;
    DataReceiverInfo dataReceiverInfo;
    //std::string inputTaskId;
};

struct scanTableInfo {
    std::string host ;// todo :current set one host one shard
    std::string dbName ;
    std::string tableName;

};
struct ScanPartition{
    UInt32 partitionId;
    std::string taskId;
    scanTableInfo info ;
};

struct ScanTaskDataSource {
    std::vector<std::string> distributeKeys;
    ScanPartition partition;
};

struct ExechangeTaskDataSource {
    //DataExechangeType type;
    int childStageId;
    std::vector<std::string> distributeKeys;
    ExechangePartition partition;
    std::vector<std::string> inputTaskIds;

};
struct ExechangeTaskDataDest {
    std::vector<std::string> distributeKeys;
    std::map<int,ExechangePartition>  partitions; // partitionId -> partition
};


class Distribution {

public:

    bool equals(Distribution *  right); // paritionNum and distributeKeys all equal
    bool keyEquals(std::vector<std::string> keys);
    Distribution();

    std::vector<std::string> distributeKeys; //exechangePartitions and scanPartitions partition num and distribute key must be the same


    int partitionNum ;

};

class ScanDistribution : public Distribution {

public:

    ScanDistribution(std::vector<std::string> keys_,int partitionNum_){
        distributeKeys = keys_;
        partitionNum = partitionNum_;
    }

    ScanDistribution(){};



    std::map<int,ScanPartition> scanPartitions;

    bool isPartitionTaskAssigned = false;

    void setScanPartitions(std::map<int,ScanPartition> part){ scanPartitions = part;isPartitionTaskAssigned = true;}


};


class ExechangeDistribution  : public Distribution{

public:
    ExechangeDistribution(std::vector<std::string> keys_,int partitionNum_){
        distributeKeys = keys_;
        partitionNum = partitionNum_;
    }
    ExechangeDistribution(){};


   std::map<int,std::map<int,ExechangePartition>> childStageToExechangePartitions; // child stageId -> (partitionId,datareceiver)

   bool isPartitionTaskAssigned = false;

   //void setExechangePartitions( std::map<int,std::vector<ExechangePartition>> part){ childStageToExechangePartitions = part;isPartitionTaskAssigned = true;}



};

}

