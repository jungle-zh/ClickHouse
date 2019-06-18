//
// Created by usser on 2019/6/17.
//
#pragma once


#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/Partition.h>
#include "Task.h"

namespace DB {

    class Stage {


    private:
        std::shared_ptr<PlanNode> exechangeSender;
        std::shared_ptr<PlanNode> exechangeReceiver;
        std::vector<std::shared_ptr<PlanNode>> planNodes;
        std::vector<std::shared_ptr<ExecNode>> execNodes;
        //std::shared_ptr<Distribution> distribution;
        std::vector<Task> tasks ;
    public:
        Stage(std::shared_ptr<PlanNode>  exechangeSender_)
        :exechangeSender(exechangeSender_){};
        void addPlanNode(std::shared_ptr<PlanNode>  node);
        void addExechangeReceiver(std::shared_ptr<PlanNode> exechangeReceiver_){
            //planNodes.push_back(exechangeReceiver_);
            exechangeReceiver = exechangeReceiver_;
            //distribution = exechangeReceiver->getDistribution();
        }
        void addChild(std::shared_ptr<Stage>  child);

        void init();
        bool isScanStage() { return  isScanStage_;}
        bool isResultStage()  { return isResultStage_ ;}
        bool isScanStage_;
        bool isResultStage_;

        //std::shared_ptr<Distribution> Distribution() { return  distribution;}

        std::shared_ptr<PlanNode> getExechangeReceiver() { return  exechangeReceiver; }
        std::shared_ptr<PlanNode> getExechangeSender() {return exechangeSender;}
        //void getExechangeReceiverDistributionPartition();




    };


}