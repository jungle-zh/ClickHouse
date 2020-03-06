//
// Created by usser on 2019/6/17.
//
#pragma once


#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/Partition.h>
#include <map>
#include "Task.h"

namespace DB {


#define INSERT_BLANK(n) \
    for(size_t i=0;i<n;++i){  \
        ss << " ";   \
    }  \

    class ScanPlanNode;
    class Stage {


    private:
        //std::shared_ptr<PlanNode> exechangeSender;
        //std::shared_ptr<PlanNode> exechangeReceiver;
        std::vector<std::shared_ptr<PlanNode>> planNodes;
        std::vector<std::shared_ptr<ExecNode>> execNodes;

        std::map<std::string,std::shared_ptr<Stage>>   childs;
        std::vector<std::string> childIds;
        std::shared_ptr<Stage> father;
        std::map<UInt32 ,std::shared_ptr<Task>> tasks; // partitionId -> task

    public:

        //static  int g_id;
        Stage(std::string jobId,int  stageid,Context * context_ ){
            stageId = jobId + "_"  + std::to_string(stageid);
            context = context_;
        };
        void addPlanNode(std::shared_ptr<PlanNode>  node) { planNodes.push_back(node);}
        void addChild(std::string childStageId,std::shared_ptr<Stage>  child){ childs.insert({childStageId,child});
        childIds.push_back(childStageId);}

        void init();

        std::string getTaskId(size_t partitionId) { return  stageId + "_" + std::to_string(partitionId); }
        std::vector<std::string> getTaskIds() ;

        //void buildTaskSourceAndDest();
        void buildTaskSource();
        void buildTaskExecNode();
        void buildTask();// convert planNode to execNode

        std::map<UInt32 ,std::shared_ptr<Task>> getTasks() { return  tasks; }

        std::shared_ptr<Task> getTask(size_t partitionId) { return  tasks[partitionId];}
        std::vector<std::string> getChildStageIds() {return  childIds;}
        std::shared_ptr<Stage> getChildStage(std::string childStageId);

        void setSourceExechangeType(DataExechangeType type_)  { sourceExechangeType = type_;}
        void setDestExechangeType(DataExechangeType type_)  { destExechangeType = type_;}
        void setFather(std::shared_ptr<Stage> father_){ father = father_; }

        ScanPlanNode *  getScanNode();
        //bool isScanStage_;
        bool hasExechange = false;
        bool hasScan = false;
        bool isResultStage_ = false;
        std::string stageId;

        //std::shared_ptr<Distribution> exechangeDistribution;
        //std::shared_ptr<Distribution> scanDistribution;
        //ExechangeDistribution *  getExechangeDistribution() { return static_cast<ExechangeDistribution * > (exechangeDistribution.get());}
        //ScanDistribution *  getScanDistribution() { return static_cast<ScanDistribution*> (scanDistribution.get());}
        //void setExechangeDistribution(std::shared_ptr<Distribution> dis ) {  exechangeDistribution = dis; }
        //void setScanDistribution(std::shared_ptr<Distribution> dis) { scanDistribution = dis ;}


        void setDistribution( std::shared_ptr<Distribution> distribution_){ distribution = distribution_;}
        std::shared_ptr<Distribution>  fatherDistribution;
        std::shared_ptr<Distribution>  distribution;
        std::map<std::string  , StageSource> stageSource ;
        ScanSource scanSource;

        size_t  getPartitionNum ();
        DataExechangeType  sourceExechangeType;
        DataExechangeType  destExechangeType;

        void addRightTableChildStageId(std::string childStageId){
            rightTableChildStageIds.push_back(childStageId);
        }
        void addMainTableChildStageId(std::string childStageId){
            mainintTableChildStageIds.push_back(childStageId);
        }
        std::vector<std::string> rightTableChildStageIds ;
        std::vector<std::string> mainintTableChildStageIds ;

        Context * context;
        void debugString(std::stringstream  & ss,size_t  blankNum) ;


    };


}