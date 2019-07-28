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
   tsortmerge,
   tdistincemerge,
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
    bool equals(DataReceiverInfo & other){
        return  ip == other.ip && dataPort == other.dataPort;
    }

    std::string ip;  //dynamic allocate 
    UInt32 dataPort;
};
struct ExechangePartition{
    UInt32 partitionId;
    std::vector<std::string> childTaskIds;
    DataExechangeType  exechangeType;
    std::string rightTableChildStageId ;
    std::vector<std::string> mainTableChildStageId;

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
    //int childStageId;
   // bool isRightTable;
    std::vector<std::string> distributeKeys;
    ExechangePartition partition;
    std::vector<std::string> inputTaskIds;
    DataReceiverInfo receiver;

};
struct ExechangeTaskDataDest {
    std::vector<std::string> distributeKeys;
    std::map<int,ExechangePartition>  partitionInfo; // partitionId -> partition
    std::map<int,DataReceiverInfo> receiverInfo;     // partitionId -> reciever
    bool isResult = false;
};


class Distribution {

public:

    bool equals(Distribution &  right) {
        if(keyEquals(right.distributeKeys) &&  partitionNum == right.partitionNum)
            return true;
        return false;
    }
    bool keyEquals(std::vector<std::string> keys){
        for(size_t i=0;i< distributeKeys.size(); ++i){
            if(distributeKeys[i] != keys[i]){
                return false;
            }
        }
        return true;
    }
    Distribution( std::vector<std::string> distributeKeys_ , int partitionNum_){
        distributeKeys = distributeKeys_;
        partitionNum = partitionNum_;
    }
    Distribution(){}



    std::vector<std::string> distributeKeys; //exechangePartitions and scanPartitions partition num and distribute key must be the same


    int partitionNum ;

};

class ScanDistribution : public Distribution {

public:

    ScanDistribution(std::vector<std::string> keys_,int partitionNum_):Distribution(keys_,partitionNum_){

    }

    ScanDistribution(){};



    std::map<int,ScanPartition> scanPartitions;

    bool isPartitionTaskAssigned = false;

    void setScanPartitions(std::map<int,ScanPartition> part){ scanPartitions = part;isPartitionTaskAssigned = true;}


};


class ExechangeDistribution  : public Distribution{

public:
    ExechangeDistribution(std::vector<std::string> keys_,int partitionNum_):Distribution(keys_,partitionNum_){

    }
    ExechangeDistribution(){};


   //std::map<int,std::map<int,ExechangePartition>> childStageToExechangePartitions; //  child stageId-> (my partitionId,datareceiver)
   std::map<int,ExechangePartition> partitionInfo ; // partitionId -> exechange
   std::map<int,DataReceiverInfo>   receiverInfo ;  // partitionId -> receiver
   bool isPartitionTaskAssigned = false;

   //void setExechangePartitions( std::map<int,std::vector<ExechangePartition>> part){ childStageToExechangePartitions = part;isPartitionTaskAssigned = true;}



};

}

