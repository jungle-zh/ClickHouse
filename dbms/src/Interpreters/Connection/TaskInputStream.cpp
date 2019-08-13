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
            throw Exception("unknow execNode to deseralize ");
        }


    }

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
    DataReceiverInfo TaskInputStream::readDataReceiverInfo(){
        DataReceiverInfo ret ;
        readStringBinary(ret.ip,*in);
        readVarUInt(ret.dataPort,*in);

        return  ret;
    }

    ScanTaskDataSource TaskInputStream::readTaskScanSource(){
        ScanTaskDataSource ret;
        size_t distributKeySize ;
        readVarUInt(distributKeySize, *in);
        for(size_t i =0;i<distributKeySize;++i){
            std::string distributeKey ;
            readStringBinary(distributeKey,*in);
            ret.distributeKeys.push_back(distributeKey);
        }
        ret.partition =  readScanPartition();

        return  ret ;
    }
    ScanPartition TaskInputStream::readScanPartition(){
        ScanPartition ret ;
        readVarUInt(ret.partitionId,*in);
        readStringBinary(ret.taskId,*in);
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

    std::shared_ptr<Task> TaskInputStream::read(){

        std::string taskId ;
        readStringBinary(taskId,*in);
        auto exechangSource  = readTaskExechangeSource();
        auto scanSource = readTaskScanSource();
        auto dest = readTaskDest();
        size_t ExecNodeNum ;
        readVarUInt(ExecNodeNum,*in);
        std::vector<std::shared_ptr<ExecNode>> nodes;
        for(size_t i=0;i < ExecNodeNum ;++i){
           auto node =  readExecNode();
            nodes.emplace_back(node);
        }

        return std::make_shared<Task>(exechangSource,scanSource,dest,nodes,taskId,context);

    }







}