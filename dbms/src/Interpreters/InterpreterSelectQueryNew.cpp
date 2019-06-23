//
// Created by jungle on 19-6-22.
//



#include "InterpreterSelectQueryNew.h"
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ResultPlanNode.h>
#include <Interpreters/PlanNode/ExechangeNode.h>

namespace DB {


    void  InterpreterSelectQueryNew::execute(std::string destIp , int destPort) {

        ExechangePartition p;
        p.dataReceiverInfo.ip = destIp;
        p.dataReceiverInfo.dataPort = destPort;

        std::shared_ptr<Stage> resultStage = std::make_shared<Stage>();//set desc ip ,address ,query id
        std::shared_ptr<PlanNode> plan =  queryAnalyzer->analyse(&query);
        std::shared_ptr<PlanNode> result  = queryAnalyzer->addResultPlanNode(plan); // result planNode at top

        queryAnalyzer->normilizePlanTree(result);
        queryAnalyzer->addExechangeNode(result);

        queryAnalyzer->removeUnusedMergePlanNode(result);

        queryAnalyzer->splitStageByExechangeNode(result,resultStage);

        taskScheduler->applyResourceAndSubmitStage(resultStage,p.dataReceiverInfo);


    }

}
