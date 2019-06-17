//
// Created by usser on 2019/6/17.
//
#pragma once


#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/ExecNode/ExecNode.h>
namespace DB {

    class Stage {


    private:
        std::vector<PlanNode> planNodes;
        std::vector<ExecNode> execNodes;
        std::shared_ptr<PlanNode::Distribution> distribution;
        Task task ;
    public:

        void init();

        std::shared_ptr<PlanNode::Distribution> Distribution() { return  distribution;}

        void convPlanToExec();




    };


}