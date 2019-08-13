//
// Created by jungle on 19-6-10.
//

#pragma  once



#include <Parsers/ASTSelectQuery.h>
#include <Interpreters/Settings.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include "Stage.h"
#include "Context.h"


namespace DB {


    class PlanNode;
    class ASTSelectQuery;
    class ASTTableJoin;
    class QueryAnalyzer {


    public:
        QueryAnalyzer(Context * context_ ,std::string jobId_): context(context_){

            settings = context_->getSettings();
            jobId = jobId_;
            stageid = 0;
        }


        struct ScopeStack;
        public:

        void getActionsImpl(const ASTPtr & ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack);

        void getRootActions(const ASTPtr & ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions);

        std::shared_ptr<PlanNode> analyse( ASTSelectQuery * query);

        std::shared_ptr<PlanNode> analyseJoin (std::shared_ptr<PlanNode> left ,std::shared_ptr<PlanNode> right,ASTTableJoin * joininfo);

        std::shared_ptr<PlanNode> analyseWhereClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseAggregate(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseHavingClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseOrderClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseLimitClause(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> analyseSelectExp(std::shared_ptr<PlanNode> shared_ptr,ASTSelectQuery *query);

        std::shared_ptr<PlanNode> addResultPlanNode(std::shared_ptr<PlanNode> root);
        void normilizePlanTree(std::shared_ptr<PlanNode> root);
        void addExechangeNode(std::shared_ptr<PlanNode> root);

        void removeUnusedMergePlanNode(std::shared_ptr<PlanNode> root);
        void splitStageByExechangeNode(std::shared_ptr<PlanNode> root, std::shared_ptr<Stage>  currentStage);


    public:

        Settings settings;

         Context * context ;
         std::string jobId;
         int stageid;

        void  collectUsedColumns( IAST *  query,std::set<std::string>  & usedColumns);
    };



}
