//
// Created by jungle on 19-6-22.
//



#include "InterpreterSelectQueryNew.h"
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ResultPlanNode.h>
#include <Interpreters/PlanNode/ExechangeNode.h>

namespace DB {


    BlockIO  InterpreterSelectQueryNew::execute() {

        //DataReceiverInfo receiverInfo(destIp,destPort);

        std::shared_ptr<Stage> resultStage = std::make_shared<Stage>(queryAnalyzer->jobId,queryAnalyzer->stageid++,context);//set desc ip ,address ,query id
        resultStage->setReslutStage();

        std::shared_ptr<PlanNode> plan =  queryAnalyzer->analyse(&query);
        std::shared_ptr<PlanNode> result  = queryAnalyzer->addResultPlanNode(plan); // result planNode at top

        queryAnalyzer->normilizePlanTree(result);
        queryAnalyzer->addExechangeNode(result);

        queryAnalyzer->removeUnusedMergePlanNode(result); // single node tree

        //todo

        if(queryAnalyzer->onlyOneStage(result)){
            queryAnalyzer->addUnionToSplitStage(result);
        }

        queryAnalyzer->splitStageByExechangeNode(result,resultStage); // single node tree to distribute task


        taskScheduler->schedule(resultStage);
        auto io =    taskScheduler->getBlockIO();
        assert(io.buffer != NULL);
        return   io;

    }

}
