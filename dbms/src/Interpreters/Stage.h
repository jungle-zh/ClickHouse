//
// Created by usser on 2019/6/17.
//
#pragma once


#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/Partition.h>
#include "Task.h"

namespace DB {



    class ScanPlanNode;
    class Stage {


    private:
        //std::shared_ptr<PlanNode> exechangeSender;
        //std::shared_ptr<PlanNode> exechangeReceiver;
        std::vector<std::shared_ptr<PlanNode>> planNodes;
        std::vector<std::shared_ptr<ExecNode>> execNodes;

        std::vector<std::shared_ptr<Stage>>   childs;
        std::shared_ptr<Stage> father;
        std::map<int,std::shared_ptr<Task>> tasks;

    public:

        Stage();
        void addPlanNode(std::shared_ptr<PlanNode>  node) { planNodes.push_back(node);}
        void addChild(std::shared_ptr<Stage>  child);
        void convetPlanToExec();

        void init();
        bool isScanStage() { return  isScanStage_;}
        bool isResultStage()  { return isResultStage_ ;}
        bool noChildStage();
        std::string getTaskId(int partitionNum) { return  std::to_string(stageId) + "_" + std::to_string(partitionNum); }

        void buildTaskSourceAndDest();
        void buildTaskExecNode();
        void buildTask();// convert planNode to execNode

        std::vector<std::shared_ptr<Task>> getTasks() { return  tasks; }
        std::vector<std::shared_ptr<Stage>> getChildStages();

        void setSourceExechangeType(DataExechangeType type);
        void setFather(std::shared_ptr<Stage> father_){ father = father_; }

        ScanPlanNode *  getScanNode();
        bool isScanStage_;
        bool isResultStage_;
        int stageId;

        std::shared_ptr<Distribution> exechangeDistribution;
        std::shared_ptr<Distribution> scanDistribution;
        ExechangeDistribution *  getExechangeDistribution() { return static_cast<ExechangeDistribution * > (exechangeDistribution.get());}
        ScanDistribution *  getScanDistribution() { return static_cast<ScanDistribution*> (scanDistribution.get());}
        void setExechangeDistribution(std::shared_ptr<Distribution> dis ) {  exechangeDistribution = dis; }
        void setScanDistribution(std::shared_ptr<Distribution> dis) { scanDistribution = dis ;}
        int  getPartitionNum ();
        DataExechangeType  sourceExechangeType;
        DataExechangeType  destExechangeType;



    };


}