//
// Created by Administrator on 2019/4/1.
//
#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class PlanNode ;

class PlanStage;

class ASTTableExpression;

class InterpreterSelectQueryToPlanNode {

    //对于 当前 ASTPtr ,如果 from 是 table ,
    //说明到叶子节点了，不需要go deeper
    //如果 from 是 join/in/union 或者 select
    // go deeper and build sub planNode, from 构建完成,
    // 在构建当前 ASTPtr planNode

public:
    struct AnalysisResult1
    {
        //bool has_join       = false;
        bool has_where      = false;
        bool need_aggregate = false;
        bool has_having     = false;
        bool has_order_by   = false;
        bool has_limit_by   = false;
        bool has_distinct   = false;

        //ExpressionActionsPtr before_join;   /// including JOIN
        ExpressionActionsPtr before_where;
        ExpressionActionsPtr before_aggregation;
        ExpressionActionsPtr before_having;
        ExpressionActionsPtr before_select;
        ExpressionActionsPtr before_order ;
        ExpressionActionsPtr before_limit_by;
        ExpressionActionsPtr final_projection;

        /// Columns from the SELECT list, before renaming them to aliases.
        Names selected_columns;

        /// Do I need to perform the first part of the pipeline - running on remote servers during distributed processing.
       // bool first_stage = false;
        /// Do I need to execute the second part of the pipeline - running on the initiating server during distributed processing.
       /// bool second_stage = false;

        //SubqueriesForSets subqueries_for_sets;
    };

    AnalysisResult1 analyzeExpressions();
    PlanNode::PlanNodePtr buildTableExpression(ASTTableExpression *  table);

    PlanNode::PlanNodePtr buildPlanNode(ASTPtr  top);

    ASTPtr  fromClauseAST(ASTPtr top ) ;

    Block initPlanNodeHeader(PlanNode::PlanNodePtr  root);

    //void createStageTree(PlanNode & root);

    //void createExecNode(PlanStage & root);

    PlanNode dealSubPlan(ASTPtr fromClause)  ;

    PlanNode::PlanNodePtr  WhereClause(ASTPtr current,Block & header);
    PlanNode::PlanNodePtr  AggClause(ASTPtr current,Block  header);
    PlanNode::PlanNodePtr  HavingClause(ASTPtr current,Block  header);
    PlanNode::PlanNodePtr  OrderByClause(ASTPtr current,Block  header);
    PlanNode::PlanNodePtr  LimitClause(ASTPtr current,Block  header);
    PlanNode::PlanNodePtr  SelectClause(ASTPtr current,Block  header);
private:
    std::unique_ptr<ExpressionAnalyzer> query_analyzer;

    AnalysisResult1 analysisResult ;


    std::shared_ptr<PlanNode>  planNodeRoot  ;


    Block header;

    Settings settings;


    PlanNode WhereClause(ASTPtr shared_ptr);


    int stageId;
};


}


