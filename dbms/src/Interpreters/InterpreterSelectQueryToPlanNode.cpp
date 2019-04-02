//
// Created by Administrator on 2019/4/1.
//
#include <Interpreters/InterpreterSelectQueryToPlanNode.h>
#include <common/logger_useful.h>
#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ProjectNode.h>
#include <Interpreters/PlanNode/LimitNode.h>
#include <Interpreters/PlanNode/OrderByNode.h>
#include <Interpreters/PlanNode/AggFirstLevelNode.h>
#include <Interpreters/PlanNode/AggSecondLevelNode.h>
#include <Interpreters/PlanNode/HavingNode.h>
#include <Interpreters/PlanNode/DistinctNode.h>
namespace DB {

InterpreterSelectQueryToPlanNode::AnalysisResult1 InterpreterSelectQueryToPlanNode::analyzeExpressions(){

    ExpressionActionsChain chain;
    //AnalysisResult1  res;
    analysisResult.need_aggregate = query_analyzer->hasAggregation();

    if(query_analyzer->appendArrayJoin(chain, false)){
        LOG_DEBUG(&Logger::get("InterpreterSelectQuery")," array join action :\n" + chain.steps.back().actions->dumpActions());
    }

   /*

    if (query_analyzer->appendJoin(chain, !res.first_stage))
    {
        res.has_join = true;
        res.before_join = chain.getLastActions();
        chain.addStep();
    }

    */

    if (query_analyzer->appendWhere(chain, false))
    {
        analysisResult.has_where = true;
        analysisResult.before_where = chain.getLastActions();
        chain.addStep();
    }

    if (analysisResult.need_aggregate)
    {
        query_analyzer->appendGroupBy(chain, false );
        query_analyzer->appendAggregateFunctionsArguments(chain, false);
        analysisResult.before_aggregation = chain.getLastActions();

        LOG_DEBUG(&Logger::get("InterpreterSelectQuery"), "ExpressionActions :before_aggregation is \n"+chain.getLastActions()->dumpActions());

        chain.finalize();
        chain.clear();

        if (query_analyzer->appendHaving(chain, false ))
        {
            analysisResult.has_having = true;
            analysisResult.before_having = chain.getLastActions();
            chain.addStep();
        }
    }

    /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
    //query_analyzer->appendSelect(chain, res.need_aggregate ? !res.second_stage : !res.first_stage);
    query_analyzer->appendSelect(chain, false);
    analysisResult.selected_columns = chain.getLastStep().required_output;
    analysisResult.before_select = chain.getLastActions();
    chain.addStep();
    if( query_analyzer->appendOrderBy(chain, false)){
        analysisResult.has_order_by = true;
        analysisResult.before_order = chain.getLastActions();
        chain.addStep();
    }


    if (query_analyzer->appendLimitBy(chain, false))
    {
        analysisResult.has_limit_by = true;
        analysisResult.before_limit_by = chain.getLastActions();
        chain.addStep();
    }

    query_analyzer->appendProjectResult(chain);
    analysisResult.final_projection = chain.getLastActions();


    LOG_DEBUG(&Logger::get("InterpreterSelectQuery"), "# chain:\n"+chain.dumpChain());


    chain.finalize();
    chain.clear();

}



void InterpreterSelectQueryToPlanNode::buildPlanNodeDepedent(PlanNode  & node){

    // scanNode
    // agg  or distinct
    // having
    // order
    // limit
    // project


    header = std::make_shared<ProjectNode>(analysisResult.before_select) ; // ProjectNode  must be set

    if(analysisResult.has_limit_by){
        auto  limitNode = std::make_shared<LimitNode>(analysisResult.before_limit_by) ;
        header->addChild(limitNode) ;
        header = limitNode ;
    }

    if(analysisResult.has_order_by){
        auto  orderByNode = std::make_shared<OrderByNode>(analysisResult.before_order);
        header->addChild(orderByNode) ;
        header = orderByNode ;
    }

    if(analysisResult.has_having){
        auto  havingNode = std::make_shared<HavingNode>(analysisResult.before_having);
        header->addChild(havingNode) ;
        header = havingNode ;

    }

    if(analysisResult.need_aggregate){

        auto  aggSecondLevelNode = std::make_shared<AggSecondLevelNode>();
        header->addChild(aggSecondLevelNode) ;
        header = aggSecondLevelNode ;


        auto  aggFirstLevelNode = std::make_shared<AggFirstLevelNode>(analysisResult.before_aggregation);
        header->addChild(aggFirstLevelNode) ;
        header = aggFirstLevelNode ;

    }else if (analysisResult.has_distinct){

        auto  distinctNode  = std::make_shared<DistinctNode>();
        header->addChild(distinctNode) ;
        header = distinctNode ;

    }


    auto  scanNode  =  std::make_shared<ProjectNode>(analysisResult.before_select) ; // ProjectNode  must be set





}


}
