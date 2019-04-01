//
// Created by Administrator on 2019/4/1.
//


namespace DB {

AnalysisResult InterpreterSelectQueryToPlanNode::analyzeExpressions(){

    ExpressionActionsChain chain;
    AnalysisResult1  res;
    res.need_aggregate = query_analyzer->hasAggregation();

    if(query_analyzer->appendArrayJoin(chain, !res.first_stage)){
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

    if (query_analyzer->appendWhere(chain, !res.first_stage))
    {
        res.has_where = true;
        res.before_where = chain.getLastActions();
        chain.addStep();
    }

    if (res.need_aggregate)
    {
        query_analyzer->appendGroupBy(chain, !res.first_stage);
        query_analyzer->appendAggregateFunctionsArguments(chain, !res.first_stage);
        res.before_aggregation = chain.getLastActions();

        LOG_DEBUG(&Logger::get("InterpreterSelectQuery"), "ExpressionActions :before_aggregation is \n"+chain.getLastActions()->dumpActions());

        chain.finalize();
        chain.clear();

        if (query_analyzer->appendHaving(chain, !res.second_stage))
        {
            res.has_having = true;
            res.before_having = chain.getLastActions();
            chain.addStep();
        }
    }

    /// If there is aggregation, we execute expressions in SELECT and ORDER BY on the initiating server, otherwise on the source servers.
    query_analyzer->appendSelect(chain, res.need_aggregate ? !res.second_stage : !res.first_stage);
    res.selected_columns = chain.getLastStep().required_output;
    res.has_order_by = query_analyzer->appendOrderBy(chain, res.need_aggregate ? !res.second_stage : !res.first_stage);
    res.before_order_and_select = chain.getLastActions();
    chain.addStep();

    if (query_analyzer->appendLimitBy(chain, !res.second_stage))
    {
        res.has_limit_by = true;
        res.before_limit_by = chain.getLastActions();
        chain.addStep();
    }

    query_analyzer->appendProjectResult(chain);
    res.final_projection = chain.getLastActions();


    LOG_DEBUG(&Logger::get("InterpreterSelectQuery"), "# chain:\n"+chain.dumpChain());

    chain.finalize();
    chain.clear();

}



void InterpreterSelectQueryToPlanNode::buildPlanNodeTree(){



}


}
