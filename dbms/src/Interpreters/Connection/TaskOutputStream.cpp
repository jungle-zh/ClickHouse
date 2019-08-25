//
// Created by jungle on 19-6-18.
//

#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExecNode/FilterExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/MergeExecNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include <Interpreters/ExecNode/ScanExecNode.h>
#include <Interpreters/ExecNode/UnionExecNode.h>
#include "TaskOutputStream.h"

namespace DB {

    void TaskOutputStream::init() {

       // out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

    }

    void TaskOutputStream::write(Task  &  task) {


        //task.getExecSources()

        //ExechangeTaskDataSource exechangeSource  = task.getExecSources();
        //ExechangeTaskDataDest dest =  task.getExecDest();
        //ScanTaskDataSource  scanSource =  task.getScanSource();
        auto stageSource = task.stageSource;
        auto scanSource  = task.scanSource;

        writeStringBinary(task.getTaskId(),*out);


        writeVarUInt(stageSource.size(),*out);
        for(auto it : stageSource){
            writeStringBinary(it.first,*out);
            write(it.second);
        }

        write(scanSource);

        write(task.getExecNodes());

        write(task.fatherDistribution);
        out->next();



    }
    void  TaskOutputStream::write(std::vector<std::shared_ptr<ExecNode>> execnodes) {

        writeVarUInt(execnodes.size(),*out);
        for(auto e : execnodes){
            writeStringBinary(e->getName(),*out);
            write(e);
        }


    }
    void  TaskOutputStream::write(std::shared_ptr<ExecNode> e) {

        if(AggExecNode * aggExecNode = typeid_cast<AggExecNode *>(e.get())){
            aggExecNode->serialize(*out);
        }else if(FilterExecNode * filterExecNode = typeid_cast<FilterExecNode *>(e.get())){
            filterExecNode->serialize(*out);
        }else if(JoinExecNode * joinExecNode = typeid_cast<JoinExecNode *>(e.get())){
            joinExecNode->serialize(*out);
        }else if(MergeExecNode * mergeExecNode = typeid_cast<MergeExecNode *>(e.get())){
            mergeExecNode->serialize(*out);
        }else if(ProjectExecNode * projectExecNode = typeid_cast<ProjectExecNode *>(e.get())){
            projectExecNode->serialize(*out);
        }else if(ScanExecNode * scanExecNode = typeid_cast<ScanExecNode *>(e.get())){
            scanExecNode->serialize(*out);
        }else if(UnionExecNode * unionExecNode = typeid_cast<UnionExecNode *>(e.get())){
            unionExecNode->serialize(*out);
        }


    }
    void TaskOutputStream::write(ScanSource & scanSource) {
        //std::vector<std::string> distributeKeys;
        //std::vector<UInt32> partitionIds;
        //std::map<UInt32,ScanPartition> partition;

        writeVarUInt(scanSource.distributeKeys.size(),*out);
        for(size_t i=0;i< scanSource.distributeKeys.size();++i){
            writeStringBinary(scanSource.distributeKeys[i],*out);
        }
        writeVarUInt(scanSource.partitionIds.size(),*out);
        for(size_t i=0;i< scanSource.partitionIds.size();++i){
            writeVarUInt(scanSource.partitionIds[i],*out);
        }
        writeVarUInt(scanSource.partition.size(),*out);
        for(auto it : scanSource.partition){
            writeVarUInt(it.first,*out);
            write(it.second);
        }


    }
    void TaskOutputStream::write(StageSource  & stageSource){
       // std::vector<std::string> newDistributeKeys;
       // std::vector<UInt32> newPartitionIds;
       // std::map<std::string,TaskSource> taskSources; //

       writeVarUInt(stageSource.newDistributeKeys.size(),*out);
       for(size_t i=0;i< stageSource.newDistributeKeys.size();++i){
           writeStringBinary(stageSource.newDistributeKeys[i],*out);
       }
       writeVarUInt(stageSource.newPartitionIds.size(),*out);
       for(size_t i=0;i< stageSource.newPartitionIds.size();++i){
           writeVarUInt(stageSource.newPartitionIds[i],*out);
       }
       writeVarUInt(stageSource.taskSources.size(),*out);
       for(auto it : stageSource.taskSources){
           writeStringBinary(it.first,*out);
           write(it.second);
       }

    }
    void TaskOutputStream::write(DB::Distribution fatherDistribution) {
        //

        writeVarUInt(fatherDistribution.distributeKeys.size(),*out);
        for(size_t i =0;i<fatherDistribution.distributeKeys.size();++i ){
            writeStringBinary(fatherDistribution.distributeKeys[i],*out);
        }
        writeVarUInt(fatherDistribution.parititionIds.size(),*out);
        for(size_t i =0;i<fatherDistribution.parititionIds.size();++i ){
            writeVarUInt(fatherDistribution.parititionIds[i],*out);
        }


    }
    /*

    void  TaskOutputStream::write(ExechangeTaskDataSource & source){
        writeVarUInt(source.distributeKeys.size(),*out);
        for(size_t i=0;i< source.distributeKeys.size();++i){
            writeStringBinary(source.distributeKeys[i],*out);
        }
        write(source.partition);
        writeVarUInt(source.inputTaskIds.size(),*out);
        for(size_t j=0;j<source.inputTaskIds.size();++j){
            writeStringBinary(source.inputTaskIds[j],*out);
        }
        write(source.receiver);

    }

    void TaskOutputStream::write(ExechangePartition & partition) {
        writeVarUInt(partition.partitionId,*out);
        writeVarUInt(partition.childTaskIds.size(),*out);
        for (size_t i = 0; i < partition.childTaskIds.size(); ++i) {
            writeStringBinary(partition.childTaskIds[i],*out);
        }
        std::string exechangeType ;
        switch (partition.exechangeType) {
            case DataExechangeType::tone2onejoin:
                exechangeType = "tone2onejoin";
                break;
            case DataExechangeType::toneshufflejoin:
                exechangeType = "toneshufflejoin";
                break;
            case DataExechangeType::ttwoshufflejoin:
                exechangeType = "ttwoshufflejoin";
                break;
            case DataExechangeType::taggmerge:
                exechangeType = "taggmerge";
                break;
            case DataExechangeType::tsortmerge:
                exechangeType = "tsortmerge";
                break;
            case DataExechangeType::tdistincemerge:
                exechangeType = "tdistincemerge";
                break;
            case DataExechangeType::tunion:
                exechangeType = "tunion";
                break;
            case DataExechangeType::tresult:
                exechangeType = "tresult";
                break;
            default:
                break;
        }
        writeStringBinary(exechangeType,*out);
        writeStringBinary(partition.rightTableChildStageId,*out);
        writeVarUInt(partition.mainTableChildStageId.size(),*out);
        for (size_t j = 0; j < partition.mainTableChildStageId.size(); ++j) {
            writeStringBinary(partition.mainTableChildStageId[j],*out);
        }



    }

    void  TaskOutputStream::write(ExechangeTaskDataDest & dest){

        writeVarUInt(dest.distributeKeys.size(),*out);
        for(size_t i =0; i< dest.distributeKeys.size(); ++i){
            writeStringBinary(dest.distributeKeys[i],*out);
        }
        writeVarUInt(dest.partitionInfo.size(),*out);
        for(auto e : dest.partitionInfo){
            writeVarUInt(e.first,*out);
            write(e.second);
        }
        writeVarUInt(dest.receiverInfo.size(),*out);
        for(auto e : dest.receiverInfo){
            writeVarUInt(e.first,*out);
            write(e.second);
        }

    }

    void  TaskOutputStream::write(ScanTaskDataSource & source){

        writeVarUInt(source.distributeKeys.size(),*out);
        for(size_t i=0;i<source.distributeKeys.size();++i){
            writeStringBinary(source.distributeKeys[i],*out);
        }
        write(source.partition);

    }
     */
    void TaskOutputStream::write(ScanPartition & partition){
        writeVarUInt(partition.partitionId,*out);
        //writeStringBinary(partition.taskId,*out);
        write(partition.info);
    }
    void TaskOutputStream::write(scanTableInfo & info){
        writeStringBinary(info.tableName, *out);
        writeStringBinary(info.dbName,*out);
        writeStringBinary(info.host,*out);
    }


       void TaskOutputStream::write(TaskSource & source){
        writeStringBinary(source.ip,*out);
        writeVarUInt(source.dataPort,*out);
    }



}