//
// Created by jungle on 19-6-10.
//

#pragma  once



#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/Settings.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/ExpressionActions.h>


namespace DB {


    class PlanNode;
    class ASTSelectQuery;
    //select query ast to planNode
    class QueryAnalyzer {

        private:

        Settings settings;
        struct ScopeStack;
        public:

        void getActionsImpl(const ASTPtr & ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack);

        void getRootActions(const ASTPtr & ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions);

        std::shared_ptr<PlanNode> analyse( ASTSelectQuery * query);

        std::shared_ptr<PlanNode> analyseWhereClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseAggregate(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseHavingClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseOrderClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseLimitClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseSelectExp(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);



    };

}
