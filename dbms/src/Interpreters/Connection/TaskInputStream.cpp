//
// Created by jungle on 19-6-18.
//

#include <Interpreters/ExecNode/ExecNode.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/MergeExecNode.h>
#include <Interpreters/ExecNode/FilterExecNode.h>
#include <Interpreters/ExecNode/UnionExecNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include <Interpreters/ExecNode/ScanExecNode.h>
#include "TaskInputStream.h"

namespace  DB {



    std::shared_ptr<ExecNode> TaskInputStream::readExecNode() {

        std::string nodeName ;
        readStringBinary(nodeName, *in);
        if (nodeName  == "aggExecNode") {
            return AggExecNode::deserialize(*in,context);
        } else if(nodeName == "joinExecNode"){
            return JoinExecNode::deserialize(*in);
        } else if(nodeName == "mergeExecNode"){
            return MergeExecNode::deserialize(*in);
        } else if(nodeName == "filterExecNode"){
            return FilterExecNode::deserialize(*in,context);
        } else if(nodeName == "unionExecNode"){
            return UnionExecNode::deseralize(*in);
        } else if(nodeName == "projectExecNode"){
            return ProjectExecNode::deseralize(*in,context);
        } else if(nodeName == "scanExecNode"){
            return ScanExecNode::deseralize(*in,context);
        } else {
            throw Exception("unknow execNode to deseralize " + nodeName);
        }


    }
    /***

    ExechangeTaskDataDest TaskInputStream::readTaskDest() {
        ExechangeTaskDataDest ret;
        size_t distributeKeySize ;
        readVarUInt(distributeKeySize,*in);
        for(size_t i=0;i< distributeKeySize;++i){
            std::string distributeKey ;
            readStringBinary(distributeKey,*in);
            ret.distributeKeys.push_back(distributeKey);
        }
        size_t partitionInfoMapSize ;
        readVarUInt(partitionInfoMapSize,*in);
        for(size_t i=0;i<partitionInfoMapSize;++i){
            size_t partionId;
            readVarUInt(partionId,*in);
            ExechangePartition partition =  readExechangePartition();
            ret.partitionInfo.insert({partionId,partition});
        }

        size_t dataReceiverInfoMapSize ;
        readVarUInt(dataReceiverInfoMapSize,*in);
        for(size_t i=0;i<dataReceiverInfoMapSize;++i){
            size_t partionId;
            readVarUInt(partionId,*in);
            DataReceiverInfo receiverInfo =  readDataReceiverInfo();
            ret.receiverInfo.insert({partionId,receiverInfo});
        }
        return  ret;

    }

    ExechangeTaskDataSource TaskInputStream::readTaskExechangeSource() {
        ExechangeTaskDataSource ret ;
        size_t distributKeySize ;
        readVarUInt(distributKeySize,*in);
        for(size_t i =0 ;i< distributKeySize ; ++i){
            std::string key;
            readStringBinary(key,*in);
            ret.distributeKeys.push_back(key);
        }
        ret.partition = readExechangePartition();
        size_t inputTaskIdsSize;
        readVarUInt(inputTaskIdsSize,*in);
        for(size_t i=0;i<inputTaskIdsSize;++i){
            std::string taskId;
            readStringBinary(taskId,*in);
            ret.inputTaskIds.push_back(taskId);
        }
        ret.receiver = readDataReceiverInfo();

        return  ret ;
    }
    ExechangePartition TaskInputStream::readExechangePartition(){
        ExechangePartition ret ;
        readVarUInt(ret.partitionId,*in);
        size_t childTaskSize ;
        readVarUInt(childTaskSize,*in);
        for(size_t i =0 ;i< childTaskSize;++i){
            std::string childTaskId;
            readStringBinary(childTaskId,*in);
            ret.childTaskIds.push_back(childTaskId);
        }
        std::string exechangeType ;
        readStringBinary(exechangeType,*in);
         if(exechangeType == "tone2onejoin") {
             ret.exechangeType = DataExechangeType::tone2onejoin;
         } else if(exechangeType == "toneshufflejoin"){
             ret.exechangeType = DataExechangeType::toneshufflejoin;
         } else if(exechangeType == "ttwoshufflejoin" ){
             ret.exechangeType = DataExechangeType::ttwoshufflejoin;
         } else if(exechangeType == "taggmerge" ){
             ret.exechangeType = DataExechangeType::taggmerge;
         } else if(exechangeType == "tsortmerge" ){
             ret.exechangeType = DataExechangeType::tsortmerge;
         } else if(exechangeType == "tdistincemerge"){
             ret.exechangeType = DataExechangeType::tdistincemerge;
         } else if(exechangeType == "tunion"){
             ret.exechangeType = DataExechangeType::tunion;
         } else if(exechangeType == "tresult") {
             ret.exechangeType = DataExechangeType::tresult;
         }

         readStringBinary(ret.rightTableChildStageId,*in);
         size_t mainTableChildStageIdSize ;
         readVarUInt(mainTableChildStageIdSize,*in);
         for(size_t i =0;i< mainTableChildStageIdSize;++i){
             std::string mainTableStageId ;
             readStringBinary(mainTableStageId,*in);
             ret.mainTableChildStageId.push_back(mainTableStageId);
         }


        return  ret;

    }
    ***/
    TaskSource  TaskInputStream::readTaskSource(){
        TaskSource ret ;
        readStringBinary(ret.ip,*in);
        readVarUInt(ret.dataPort,*in);
        readStringBinary(ret.taskId,*in);

        return  ret;
    }
    StageSource TaskInputStream::readStageSource(){

         StageSource source;
         size_t  keyNum ;
         size_t  partitionNum;
         size_t  taskSourcesNum;
         readVarUInt(keyNum,*in);
         for(size_t i=0;i< keyNum; ++i){
             std::string key ;
             readStringBinary(key,*in);
             source.newDistributeKeys.push_back(key);
         }
         readVarUInt(partitionNum,*in);
         for(size_t i=0;i< partitionNum;++i){
             UInt32 partitionId ;
             readVarUInt(partitionId,*in);
             source.newPartitionIds.push_back(partitionId);
         }
         readVarUInt(taskSourcesNum,*in);
         for(size_t i=0;i<taskSourcesNum;++i){
             std::string taskId ;
             readStringBinary(taskId,*in);
             TaskSource taskSource  = readTaskSource();
             source.taskSources.insert({taskId,taskSource});

         }
        return  source;


    }

    ScanSource TaskInputStream::readScanSource(){
        ScanSource source;

        size_t  keyNum ;
        size_t  partitionIdNum;
        size_t  partitionNum;
        readVarUInt(keyNum,*in);
        for(size_t i=0;i< keyNum; ++i){
            std::string key ;
            readStringBinary(key,*in);
            source.distributeKeys.push_back(key);
        }
        readVarUInt(partitionIdNum,*in);
        for(size_t i=0;i< partitionIdNum;++i){
            UInt32 partitionId ;
            readVarUInt(partitionId,*in);
            source.partitionIds.push_back(partitionId);
        }
        readVarUInt(partitionNum,*in);
        for(size_t i=0;i<partitionNum;++i){
            UInt32  partitionId;
            readVarUInt(partitionId,*in);
            ScanPartition scanPartition  = readScanPartition();
            source.partition.insert({partitionId,scanPartition});

        }
        return  source;

    }

    ScanPartition TaskInputStream::readScanPartition(){
        ScanPartition ret ;
        readVarUInt(ret.partitionId,*in);
        //readStringBinary(ret.taskId,*in);
        ret.info = readScanTableInfo();
        return  ret;

    }
    scanTableInfo TaskInputStream::readScanTableInfo(){
        scanTableInfo ret ;
        readStringBinary(ret.host,*in);
        readStringBinary(ret.dbName,*in);
        readStringBinary(ret.tableName,*in);
        return  ret;
    }
    Distribution TaskInputStream::readFatherDistribution(){
        //std::vector<std::string> distributeKeys;

        //std::vector<UInt32> parititionIds;

        Distribution ret ;
        size_t keyNum ;
        size_t partitionIdNum;
        readVarUInt(keyNum,*in);
        readVarUInt(partitionIdNum,*in);

        for(size_t i=0;i<keyNum;++i){
            std::string key;
            readStringBinary(key,*in);
            ret.distributeKeys.push_back(key);
        }

        for(size_t i=0;i<partitionIdNum;++i){
            size_t partitionId;
            readVarUInt(partitionId,*in);
            ret.parititionIds.push_back(partitionId);
        }
        return  ret ;


    }
    std::vector<std::string> TaskInputStream::readStageIds(){
        std::vector<std::string> res;
        size_t stageIdNum;
        readVarUInt(stageIdNum,*in);
        for(size_t i=0;i< stageIdNum;++i){
            std::string stageId;
            readStringBinary(stageId,*in);
            res.push_back(stageId);
        }
        return  res;

    }

    std::shared_ptr<Task> TaskInputStream::read(){

        std::string taskId ;
        readStringBinary(taskId,*in);

        std::map<std::string  , StageSource>  stageSources;
        ScanSource scanSource;
        size_t stageSourceNum ;

        readVarUInt(stageSourceNum,*in);
        for(size_t i=0;i<stageSourceNum;++i){
            std::string stageId;
            readStringBinary(stageId,*in);
            StageSource source = readStageSource();
            stageSources.insert({stageId,source});
        }

        scanSource = readScanSource();

        size_t ExecNodeNum ;
        readVarUInt(ExecNodeNum,*in);
        std::vector<std::shared_ptr<ExecNode>> nodes;
        for(size_t i=0;i < ExecNodeNum ;++i){
           auto node =  readExecNode();
            nodes.emplace_back(node);
        }

        std::string hasScan ;
        std::string hasExechange ;
        std::string isResult ;
        bool  hasScan_ = false;
        bool  hasExechange_ = false;
        bool  isResult_ = false;

        readStringBinary(hasScan,*in);
        readStringBinary(hasExechange,*in);
        readStringBinary(isResult, *in);
        if(hasScan == "hasScan")
            hasScan_ = true;
        if(hasExechange == "hasExechange")
            hasExechange_ = true;
        if(isResult == "isResult")
            isResult_ = true;

        std::vector<std::string> mainTableStageIds = readStageIds();
        std::vector<std::string> hashTableStageIds = readStageIds();
        Distribution f  = readFatherDistribution();
        std::shared_ptr<Distribution> fatherDis = std::make_shared<Distribution>(f.distributeKeys,f.parititionIds);
        return std::make_shared<Task>(fatherDis,stageSources,scanSource,nodes,taskId,mainTableStageIds,hashTableStageIds,hasScan_,hasExechange_,isResult_,context);

    }







}