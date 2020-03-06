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

   tone2onejoin, //jungle comment ,still shuffle  to dest
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

    TaskReceiverInfo(){}
    TaskReceiverInfo(std::string ip_,UInt32 port_){

        ip = ip_;
        taskPort = port_;
    }
    std::string ip; // is static
    UInt32 taskPort;

};
/*
struct DataReceiverInfo {
    bool equals(DataReceiverInfo & other){
        return  ip == other.ip && dataPort == other.dataPort;
    }

    std::string ip;  //dynamic allocate 
    UInt16 dataPort;
};
 */
/*
struct ExechangePartition{
    UInt32 partitionId;
    std::vector<std::string> childTaskIds;
    DataExechangeType  exechangeType;
    std::string rightTableChildStageId ;
    std::vector<std::string> mainTableChildStageId;

};
 */

struct scanTableInfo {
    scanTableInfo(){}
    std::string host ;// todo :current set one host one shard
    std::string dbName ;
    std::string tableName;

};
struct ScanPartition{
    ScanPartition(){}
    UInt32 partitionId;
    //std::string taskId;
    scanTableInfo info ;
};

struct ScanSource {
    ScanSource(){}
    std::vector<std::string> distributeKeys;
    std::vector<UInt32> partitionIds;
    std::map<UInt32,ScanPartition> partition;
};


struct TaskSource {

    bool equals(TaskSource & other){
        return  ip == other.ip && dataPort == other.dataPort;
    }
    TaskSource(){

    }
    std::string taskId;
    std::string ip;  //dynamic allocate
    UInt16 dataPort;
};
struct StageSource {
    StageSource(){}
    std::vector<std::string> newDistributeKeys;
    std::vector<UInt32> newPartitionIds;
    std::map<std::string,TaskSource> taskSources; //
};

struct ExechangeDataSource {

    std::map<std::string  , StageSource> stageSource ;

};
/*
struct ExechangeTaskDataDest {
    std::vector<std::string> distributeKeys;
    std::map<int,ExechangePartition>  partitionInfo; // partitionId -> partition
    std::map<int,DataReceiverInfo> receiverInfo;     // partitionId -> reciever
   // bool isResult = false;
};
*/

class Distribution {

public:

    bool equals(Distribution &  right) {
        if(keyEquals(right.distributeKeys) &&  partitionEquals(right.parititionIds))
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
    bool partitionEquals(std::vector<UInt32 > ids){
        for(size_t i=0;i< parititionIds.size(); ++i){
            if(parititionIds[i] != ids[i]){
                return false;
            }
        }
        return true;
    }


    Distribution( std::vector<std::string> distributeKeys_ , size_t partitionNum){
        distributeKeys = distributeKeys_;
        for(size_t i=0;i<partitionNum;++i){
            parititionIds.push_back(i);
        }
    }

    Distribution( std::vector<std::string> distributeKeys_ , std::vector<UInt32>  parititionIds_){
        distributeKeys = distributeKeys_;
        parititionIds = parititionIds_;
    }
    Distribution(){}



    std::vector<std::string> distributeKeys; //exechangePartitions and scanPartitions partition num and distribute key must be the same

    std::vector<UInt32> parititionIds;
    //size_t partitionNum ;

};

/*
class ScanDistribution : public Distribution {

public:

    ScanDistribution(std::vector<std::string> keys_,int partitionNum_):Distribution(keys_,partitionNum_){

    }

    ScanDistribution(){};



    std::map<int,ScanPartition> scanPartitions;

    bool isPartitionTaskAssigned = false;
    std::map<int, ScanPartition> & getScanPartitions() { return  scanPartitions;}

    //void setScanPartitions(std::map<int,ScanPartition> part){ scanPartitions = part;isPartitionTaskAssigned = true;}


};
 */


/*
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
*/
}

