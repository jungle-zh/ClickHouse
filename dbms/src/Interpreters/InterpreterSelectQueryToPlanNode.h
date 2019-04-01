//
// Created by Administrator on 2019/4/1.
//

#ifndef CLICKHOUSE_INTERPRETERSELECTQUERYTOPLANNODE_H
#define CLICKHOUSE_INTERPRETERSELECTQUERYTOPLANNODE_H

namespace DB {


class InterpreterSelectQueryToPlanNode {



    struct AnalysisResult1
    {
        //bool has_join       = false;
        bool has_where      = false;
        bool need_aggregate = false;
        bool has_having     = false;
        bool has_order_by   = false;
        bool has_limit_by   = false;

        //ExpressionActionsPtr before_join;   /// including JOIN
        ExpressionActionsPtr before_where;
        ExpressionActionsPtr before_aggregation;
        ExpressionActionsPtr before_having;
        ExpressionActionsPtr before_order_and_select;
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

    AnalysisResult1 analyzeExpressions(QueryProcessingStage::Enum from_stage);

    void buildPlanNodeTree(PlanNode & root);

    void buildPlanNodeHeader(PlanNode & root);

    void createStageTree(PlanNode & root);

    void createExecNode(PlanStage & root);
};


}

#endif //CLICKHOUSE_INTERPRETERSELECTQUERYTOPLANNODE_H
