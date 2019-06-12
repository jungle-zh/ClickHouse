//
// Created by Administrator on 2019/4/1.
//
#include <Interpreters/InterpreterSelectQueryToPlanNode.h>
#include <common/logger_useful.h>

#include <Interpreters/PlanNode/ProjectNode.h>
#include <Interpreters/PlanNode/LimitNode.h>
#include <Interpreters/PlanNode/SortPartialNode.h>
#include <Interpreters/PlanNode/AggPartialNode.h>
#include <Interpreters/PlanNode/AggMergeNode.h>
#include <Interpreters/PlanNode/HavingNode.h>
#include <Interpreters/PlanNode/DistinctNode.h>
#include <Interpreters/PlanNode/ScanNode.h>
#include <Interpreters/PlanNode/FilterNode.h>
#include <Interpreters/PlanNode/ExchangeNode.h>
#include <Interpreters/PlanNode/JoinNode.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Exec/Task.h>
#include <Interpreters/Exec/Stage.h>


namespace DB {

InterpreterSelectQueryToPlanNode::AnalysisResult1 InterpreterSelectQueryToPlanNode::analyzeExpressions(){

    ExpressionActionsChain chain;
    //AnalysisResult1  res;
    //query_analyzer->init();



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


PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::buildTableExpression(ASTTableExpression * table){

    PlanNode::PlanNodePtr ret ;

    if(table->database_and_table_name){ //table name

    }else if ( table->table_function){

    }else { // subquery

        ASTSelectWithUnionQuery * unionQuery = typeid_cast<ASTSelectWithUnionQuery *>( table->subquery->children[0].get() );

        ASTExpressionList * expressionList =  typeid_cast<ASTExpressionList *>(unionQuery->list_of_selects.get());

        ASTSelectQuery * selectQuery =  typeid_cast<ASTSelectQuery *>(expressionList->children[0].get()); // union all

        assert(selectQuery != NULL);
        ret = buildPlanNode(expressionList->children[0]);

    }
    return ret ;
}

PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::buildPlanNode(ASTPtr  currentAST){

    // project
    // limit
    // order
    // having
    // agg  or distinct
    // from -> {
    //     table
    //     subquery
    //     join
    // }

    // if table
    // if join
    // if select query


    if(const ASTSelectQuery * query =  typeid_cast<const ASTSelectQuery *>(currentAST.get())) {

        //ASTPtr afterFrom   = fromClauseAST(currentAST);

        //ASTTablesInSelectQuery  query->tables
        ASTTablesInSelectQueryElement * tableElement = typeid_cast<ASTTablesInSelectQueryElement *>(query->tables->children[0].get());

        PlanNode::PlanNodePtr subPlanNode ;

        if(tableElement->table_expression) { // table or subquery

            ASTTableExpression * table = typeid_cast<ASTTableExpression *>(tableElement->table_expression.get());
            subPlanNode =  buildTableExpression(table);

        }else if (tableElement->table_join){

            ASTTableEnhanceJoin * enhanceJoin =  typeid_cast<ASTTableEnhanceJoin *>(tableElement->table_join.get());
            ASTTableExpression * leftExpression =  typeid_cast <ASTTableExpression * >(enhanceJoin->left_table_expression.get());
            ASTTableExpression * rightExpression =  typeid_cast <ASTTableExpression * >(enhanceJoin->right_table_expression.get());
            PlanNode::PlanNodePtr leftPlanNode =  buildTableExpression(leftExpression);
            PlanNode::PlanNodePtr rightPlanNode =  buildTableExpression(rightExpression);
            JoinPtr join ;
            if (enhanceJoin->using_expression_list)
            {
                Names join_key_names_left;
                Names join_key_names_right;
                ASTExpressionList & keys = typeid_cast<ASTExpressionList &>(*(enhanceJoin->using_expression_list));
                for (const auto & key : keys.children)
                {
                    if (join_key_names_left.end() == std::find(join_key_names_left.begin(), join_key_names_left.end(), key->getColumnName()))
                        join_key_names_left.push_back(key->getColumnName());
                    else
                        throw Exception("Duplicate column " + key->getColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);

                    if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), key->getAliasOrColumnName()))
                        join_key_names_right.push_back(key->getAliasOrColumnName());
                    else
                        throw Exception("Duplicate column " + key->getAliasOrColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);
                }
                join  = std::make_shared<Join>(
                        join_key_names_left, join_key_names_right,
                        settings.join_use_nulls, SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
                        query->kind, query->strictness);

            }
            subPlanNode = std::make_shared<JoinNode>(join);

            subPlanNode->addChild(leftPlanNode);
            subPlanNode->addChild(rightPlanNode);
        }
        Block columnFromSubNode = subPlanNode->getHeader();


        // input column comes from subquery output
        // subquery execute

        //alias column name and table  eg: t1.age

        //get all header needed for the select query

        Block columnNeed = CollectColumnNeed();  // origin name

        Block sourceColumn = CheckSource(columnFromSubNode,columnNeed); // origin name

        Block aliasedColumn  = MapToOutput(sourceColumn);


        PlanNode::PlanNodePtr fileterNode = WhereClause(currentAST,subPlanNode->getHeader());//header not change

        PlanNode::PlanNodePtr aggNodeOne = AggClause(currentAST,fileterNode->getHeader()); // header all change

        PlanNode::PlanNodePtr aggNodeTwo = AggClauseTwo(currentAST,aggNodeOne->getHeader());
        //保证不管是本地还是分布式执行时候其他机器上拿到输入header 是完全一样的
        //区别只有分布式执行先序列化然后反序列化，序列化之前和反序列化之后的header 一样

        PlanNode::PlanNodePtr filterNode1 = HavingClause(currentAST,aggNodeTwo->getHeader()); // header not change

        PlanNode::PlanNodePtr orderPartialNode = OrderByClause(currentAST,filterNode1->getHeader()); // header not change
        PlanNode::PlanNodePtr orderFinalNode = std::make_shared<PlanNode>();
        PlanNode::PlanNodePtr limitNode = LimitClause(currentAST,orderFinalNode->getHeader()); // header not change

        PlanNode::PlanNodePtr projectNode = SelectClause(currentAST,limitNode->getHeader()); //


        //out put is from input , but filter some unused  column

    }else {

        throw  Exception();
    }

}



void InterpreterSelectQueryToPlanNode::planNodeToDistributeTask(PlanNode::PlanNodePtr root){



    std::shared_ptr<Stage> rootStage =  std::make_shared<Stage>(stageId++); // result Stage
    // root Stage is result stage
    // if(order by ) add merge sort node to rootStage
    // if(limit ) add limit node to rootStage
    // if(order by ) add partial sort node to root
    buildStageTree(root,rootStage);


    // buildStageExchageNode( rootStage );

    buildStageTask( rootStage );

}



void InterpreterSelectQueryToPlanNode::submitStage(std::shared_ptr<Stage> currentStage){

    // submitTask , one exchange node input  correspond to one task
    // eg:
    //  shuffle join(3 exchange node )
    //    |    |    |
    //   / \  / \  / \
    //  agg (1 exchange node )
    //    |
    //  / | \


    //对于 底层 stage 会submit 到 数据所在节点
    //对于 中层 stage 会submit 到 负载低节点
    // result stage 会 submit 到 客户端节点
    // task 在哪个节点 ,input exchange 就确定在哪个节点

    currentStage->submitTasks(); //先提交上层stage ,上层 input exchange node 确定了,那么下层 output exchange 就确定了

    for(std::shared_ptr<Stage> child : currentStage->getChilds()){

        submitStage(child );
    }



}


void InterpreterSelectQueryToPlanNode::buildStageExchageNode(std::shared_ptr<Stage> curStage ){

     if(curStage->getChilds().size() == 1 ){ // agg exchage

         //one agg exchage node
         std::shared_ptr<ExchangeNode> exchange = std::make_shared<ExchangeNode>(curStage);

         curStage->addInput(exchange);

         curStage->getChilds()[0]->addOutput(exchange);
     }else if(curStage->getChilds().size() == 2){  // shuffle join exchage

         int  hashPartition = curStage->getHashPartitionNum();

         for(int i=0;i<hashPartition ;++i ){
             std::shared_ptr<ExchangeNode> exchange = std::make_shared<ExchangeNode>(curStage);

             curStage->addInput(exchange);

             curStage->getChilds()[0]->addOutput(exchange);

             curStage->getChilds()[1]->addOutput(exchange);
         }


     }

     for(std::shared_ptr<Stage> child : curStage->getChilds() ){

         buildStageExchageNode(child);
     }




}


void InterpreterSelectQueryToPlanNode::buildStageTask(std::shared_ptr<Stage>  curStage){

    if(curStage->type() == "resultStage"){
        curStage->buildResultTask();  // only one
    }else if(curStage->type() == "middleStage"){
        curStage->buildMiddleTask(); // has task id ,one(agg ,result ) or more(shuffle join)
    }else { // "scan task  there is no task id  "
        curStage->buildScanTask(); //one input exchange node to one task ,one task to all the out exchange node
        // if contains agg2 or mergeSort ,task num is 1 ,if contains shuffle join ,task num is hash join partiton num
    }
    for(auto child: curStage->getChilds()){
        buildStageTask(child);
    }

    // build task deal with input exchange node , output exchange node, planNode

}


void InterpreterSelectQueryToPlanNode::buildStageTree(PlanNode::PlanNodePtr root , std::shared_ptr<Stage>  curStage){


     if (root->getName() == "shuffleJoin"){

         curStage->addNode(root) ;

         std::shared_ptr<Stage> left =  std::make_shared<Stage>(stageId++);
         std::shared_ptr<Stage> right =  std::make_shared<Stage>(stageId++);

         curStage->addChild(left);
         curStage->addChild(right);
         std::shared_ptr<PlanNode> leftNode = root->getLeftChild() ;
         std::shared_ptr<PlanNode> rightNode = root->getRightChild() ;
         root->clearChild();  // in the stage , bottom node has no child

         buildStageTree(leftNode ,left);
         buildStageTree(rightNode,right);

     } else if(root->getName() == "AggMerge" ){


         curStage->addNode(root) ;
         std::shared_ptr<Stage> childStage =  std::make_shared<Stage>(stageId++);

         curStage->addChild(childStage);

         std::shared_ptr<PlanNode> childNode = root->getUnaryChild() ;
         root->clearChild();
         buildStageTree(childNode,childStage);

     } else if (root->getName() == "SortMerge" ){

         curStage->addNode(root);
         std::shared_ptr<Stage> childStage =  std::make_shared<Stage>(stageId++);

         curStage->addChild(childStage);

         std::shared_ptr<PlanNode> childNode = root->getUnaryChild() ;
         root->clearChild();
         buildStageTree(childNode,childStage);

     } else {
         curStage->addNode(root) ;
         buildStageTree(root->getUnaryChild(),curStage);
     }


}

void InterpreterSelectQueryToPlanNode::submitStage(std::shared_ptr<Stage> rootStage){

    //send stage tasks id with task output position(father stage task input) to different server to execute tasks

    // 同一个stage 的不同 task 可以调度到不同节点
    // scanStage 的task 调度到所有节点
    // 父 task 的输入位置要不就是agg ,sort, result 的情况 ，要不就是shuffle join 的情况
    if(rootStage->type()  == "scanStage"){
        //submit task to all server ;


    } else if( rootStage->type() == "resultStage") {
        //submit one task to result server , output send to client
    } else if( rootStage->type() == "AggMergeStage"){
        //submit one task to one server ,outout send to father stage task , if father stage type
        // is AggMergeStage  or SortMergeStage or resultStage ,there is only one task input node ,
        // if father stage is shuffleJoinStage , there is serval task input node
    } else if( rootStage->type() == "SortMergeStage") {

    } else if( rootStage->type() == "ShuffleJoinStage"){

    }

  for(auto child : rootStage->getChilds()){
      submitStage(child);
  }

}

Block  InterpreterSelectQueryToPlanNode::initPlanNodeHeader(PlanNode::PlanNodePtr  root){

    header = root->initHeader();

}


PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::WhereClause(ASTPtr current,Block & header) {


    //optimize ?
    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);


    auto  whereExpressionActions = std::make_shared<ExpressionActions> ( header.getNamesAndTypesList(),settings);
    query_analyzer->getRootActions(query.where_expression, true, false,whereExpressionActions);

    return  std::make_shared<FilterNode>( whereExpressionActions);

}




PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::AggClause(ASTPtr current,Block  & header){

    //optimize ?
    ASTSelectQuery * select_query = typeid_cast<ASTSelectQuery *>(current.get());
    query_analyzer->analyzeAggregation(select_query,header);

    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);
    auto  aggExpressionActions = std::make_shared<ExpressionActions> ( header.getNamesAndTypesList(),settings);
    ASTs asts = query.group_expression_list->children;
    for (size_t i = 0; i < asts.size(); ++i)
    {
        //step.required_output.push_back(asts[i]->getColumnName());
        query_analyzer->getRootActions(asts[i], true, false, aggExpressionActions);
    }


    query_analyzer->getActionsBeforeAggregation(query.select_expression_list, aggExpressionActions, true);

    if (query.having_expression)
        query_analyzer->getActionsBeforeAggregation(query.having_expression, aggExpressionActions, true);

    if (query.order_expression_list)
        query_analyzer->getActionsBeforeAggregation(query.order_expression_list, aggExpressionActions, true);



    aggExpressionActions->execute(header);

    //now header and aggregates are all prepared

    Names key_names;
    AggregateDescriptions aggregates;
    //todo need to get aggregateInfo
    query_analyzer->getAggregateInfo(key_names, aggregates);

    //Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
        keys.push_back(header.getPositionByName(name));
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(header.getPositionByName(name));

    //const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    //bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;
    bool allow_to_use_two_level_group_by = true;
    Aggregator::Params params(header, keys, aggregates,
                              overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                              settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                              context.getTemporaryPath());

    return  std::make_shared<AggPartialNode>(std::shared_ptr<ExpressionActions> aggExpressionActions,std::move(params)); //params include header and aggregates



}



PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::HavingClause(ASTPtr current,Block  & header){

    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);
    auto  havingExpressionActions = std::make_shared<ExpressionActions> (header.getNamesAndTypesList(),settings);
    query_analyzer->getRootActions(query.having_expression, true, false,havingExpressionActions);

    return  std::make_shared<FilterNode>( havingExpressionActions);

}
SortDescription InterpreterSelectQueryToPlanNode::getSortDescription( ASTSelectQuery & query){
    SortDescription order_descr;
    order_descr.reserve(query.order_expression_list->children.size());
    for (const auto & elem : query.order_expression_list->children)
    {
        String name = elem->children.front()->getColumnName();
        const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

        std::shared_ptr<Collator> collator;
        if (order_by_elem.collation)
            collator = std::make_shared<Collator>(typeid_cast<const ASTLiteral &>(*order_by_elem.collation).value.get<String>());

        order_descr.emplace_back(name, order_by_elem.direction, order_by_elem.nulls_direction, collator);
    }

    return order_descr;
}
size_t InterpreterSelectQueryToPlanNode::getLimitForSorting(){
    /// Partial sort can be done if there is LIMIT but no DISTINCT or LIMIT BY.
    size_t limit = 0;
    if (!query.distinct && !query.limit_by_expression_list)
    {
        size_t limit_length = 0;
        size_t limit_offset = 0;
        getLimitLengthAndOffset(query, limit_length, limit_offset);
        limit = limit_length + limit_offset;
    }

    return limit;
}


PlanNode::PlanNodePtr InterpreterSelectQueryToPlanNode::OrderByClause(ASTPtr current,Block  & header) {

    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);
    auto  orderByExpressionActions = std::make_shared<ExpressionActions> (header.getNamesAndTypesList(),settings);
    query_analyzer->getRootActions(query.order_expression_list, true, false,orderByExpressionActions);


    SortDescription order_descr = getSortDescription(query);
    size_t limit = getLimitForSorting(query);
    return  std::make_shared<PartialSortNode>( orderByExpressionActions,order_descr,limit);

}


void InterpreterSelectQueryToPlanNode::getLimitLengthAndOffset(ASTSelectQuery & query, size_t & length, size_t & offset){
    length = 0;
    offset = 0;
    if (query.limit_length)
    {
        length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
        if (query.limit_offset)
            offset = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_offset).value);
    }
}

PlanNode::PlanNodePtr  InterpreterSelectQueryToPlanNode::LimitClause(DB::ASTPtr current,Block header ) {

    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);
    size_t limit_length = 0;
    size_t limit_offset = 0;
    getLimitLengthAndOffset(query, limit_length, limit_offset);

    return  std::make_shared<LimitNode>( limit_length,limit_offset);

}


PlanNode::PlanNodePtr  InterpreterSelectQueryToPlanNode::SelectClause(DB::ASTPtr current,Block header) {

    ASTSelectQuery  query = typeid_cast<ASTSelectQuery &>(*current);
    auto  selectExpressionActions = std::make_shared<ExpressionActions> (header.getNamesAndTypesList(),settings);
    query_analyzer->getRootActions(query.select_expression_list, true, false,selectExpressionActions);

    return  std::make_shared<SelectNode>( selectExpressionActions);
}






}
