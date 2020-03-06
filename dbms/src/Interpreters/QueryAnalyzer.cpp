//
// Created by jungle on 19-6-10.
//
#include <Interpreters/QueryAnalyzer.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/PlanNode/JoinPlanNode.h>
#include <Interpreters/PlanNode/UnionPlanNode.h>
#include <Interpreters/PlanNode/FromClauseNode.h>
#include <Interpreters/PlanNode/FilterPlanNode.h>
#include <Interpreters/PlanNode/AggPlanNode.h>
#include <Interpreters/AggregateDescription.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Parsers/ASTIdentifier.h>
#include <DataTypes/DataTypeFunction.h>
#include <Interpreters/PlanNode/ExechangeNode.h>
#include <Interpreters/PlanNode/MergePlanNode.h>
#include <Interpreters/PlanNode/ScanPlanNode.h>
#include <Interpreters/PlanNode/ProjectPlanNode.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/PlanNode/ResultPlanNode.h>
#include <Parsers/queryToString.h>
#include "Stage.h"
#include "convertFieldToType.h"


namespace DB {




    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
        extern const int UNKNOWN_IDENTIFIER;
        extern const int CYCLIC_ALIASES;
        extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
        extern const int TOO_MANY_ROWS;
        extern const int NOT_FOUND_COLUMN_IN_BLOCK;
        extern const int INCORRECT_ELEMENT_OF_SET;
        extern const int ALIAS_REQUIRED;
        extern const int EMPTY_NESTED_TABLE;
        extern const int NOT_AN_AGGREGATE;
        extern const int UNEXPECTED_EXPRESSION;
        extern const int DUPLICATE_COLUMN;
        extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
        extern const int ILLEGAL_AGGREGATION;
        extern const int SUPPORT_IS_DISABLED;
        extern const int TOO_DEEP_AST;
        extern const int TOO_BIG_AST;
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

    struct QueryAnalyzer::ScopeStack {
        struct Level {
            ExpressionActionsPtr actions;
            NameSet new_columns;
        };

        using Levels = std::vector<Level>;

        Levels stack;
        Settings settings;

        ScopeStack(const ExpressionActionsPtr &actions, const Settings &settings_)
                : settings(settings_) {
            stack.emplace_back();
            stack.back().actions = actions;

            const Block &sample_block = actions->getSampleBlock();
            for (size_t i = 0, size = sample_block.columns(); i < size; ++i)
                stack.back().new_columns.insert(sample_block.getByPosition(i).name);
        }

        void pushLevel(const NamesAndTypesList &input_columns) {
            stack.emplace_back();
            Level &prev = stack[stack.size() - 2];

            ColumnsWithTypeAndName all_columns;
            NameSet new_names;

            for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it) {
                all_columns.emplace_back(nullptr, it->type, it->name);
                new_names.insert(it->name);
                stack.back().new_columns.insert(it->name);
            }

            const Block &prev_sample_block = prev.actions->getSampleBlock();
            for (size_t i = 0, size = prev_sample_block.columns(); i < size; ++i) {
                const ColumnWithTypeAndName &col = prev_sample_block.getByPosition(i);
                if (!new_names.count(col.name))
                    all_columns.push_back(col);
            }

            stack.back().actions = std::make_shared<ExpressionActions>(all_columns, settings);
        }

        size_t getColumnLevel(const std::string &name) {
            for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
                if (stack[i].new_columns.count(name))
                    return i;

            throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
        }

        void addAction(const ExpressionAction &action) {
            size_t level = 0;
            Names required = action.getNeededColumns();
            for (size_t i = 0; i < required.size(); ++i)
                level = std::max(level, getColumnLevel(required[i]));

            Names added;
            stack[level].actions->add(action, added);

            stack[level].new_columns.insert(added.begin(), added.end());

            for (size_t i = 0; i < added.size(); ++i) {
                const ColumnWithTypeAndName &col = stack[level].actions->getSampleBlock().getByName(added[i]);
                for (size_t j = level + 1; j < stack.size(); ++j)
                    stack[j].actions->addInput(col);
            }
        }

        ExpressionActionsPtr popLevel() {
            ExpressionActionsPtr res = stack.back().actions;
            stack.pop_back();
            return res;
        }

        const Block &getSampleBlock() const {
            return stack.back().actions->getSampleBlock();
        }
    };


    std::shared_ptr<PlanNode> QueryAnalyzer::analyse(DB::ASTSelectQuery *query) {

        size_t cnum = query->tables->children.size();


        std::shared_ptr<PlanNode> fromClauseNode = std::make_shared<FromClauseNode>();

        if (cnum == 1) {

            std::shared_ptr<PlanNode> childNode;
            ASTTablesInSelectQueryElement *child = typeid_cast<ASTTablesInSelectQueryElement *>(
                    query->tables->children[0].get());
            ASTTableExpression *exp = typeid_cast<ASTTableExpression *>(child->table_expression.get());

            if (exp->subquery) {

                childNode = std::make_shared<UnionPlanNode>();

                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        exp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    auto subqueryRoot = analyse(selectQuery); //todo  header map to alias_table_name.column_name and  check unique
                    childNode->addChild(subqueryRoot);
                    childNode->setHeader(subqueryRoot->getHeader());// UnionPlanNode is header is child's header
                }


            } else if (exp->database_and_table_name) {

                auto identifier = static_cast<const ASTIdentifier &>(*exp->database_and_table_name);
                std::string dbName ;
                std::string tableName ;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

                    dbName = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
                    tableName = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
                } else {
                    tableName = identifier.name;
                }


                std::set<std::string>  usedColumn;
                collectUsedColumns(query,usedColumn);

                childNode = std::make_shared<ScanPlanNode>(dbName,tableName,usedColumn,queryToString(*query),context);

            } else if (exp->table_function) {

            } else {
                throw Exception("unknow table Expression");
            }

            fromClauseNode->addChild(childNode);

            fromClauseNode->setHeader(childNode->getHeader());

        } else if (cnum == 2) {

            std::shared_ptr<PlanNode> leftNode;
            std::shared_ptr<PlanNode> rightNode;

            ASTTablesInSelectQueryElement *left = typeid_cast<ASTTablesInSelectQueryElement *>(
                    query->tables->children[0].get());
            ASTTablesInSelectQueryElement *right = typeid_cast<ASTTablesInSelectQueryElement *>(
                    query->tables->children[1].get());


            ASTTableExpression *leftexp = typeid_cast<ASTTableExpression *>(left->table_expression.get());
            ASTTableExpression *rightexp = typeid_cast<ASTTableExpression *>(right->table_expression.get());
            if (leftexp->subquery) {

                leftNode = std::make_shared<UnionPlanNode>();
                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        leftexp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    auto subqueryRoot = analyse(selectQuery);
                    leftNode->addChild(subqueryRoot);
                    leftNode->setHeader(subqueryRoot->getHeader());
                }

            } else if (leftexp->database_and_table_name) {


                auto identifier = static_cast<const ASTIdentifier &>(*leftexp->database_and_table_name);
                std::string dbName ;
                std::string tableName ;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

                    dbName = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
                    tableName = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
                }


                std::set<std::string>  usedColumn;
                collectUsedColumns(query,usedColumn);

                leftNode = std::make_shared<ScanPlanNode>(dbName,tableName,usedColumn,queryToString(*left),context);
                //leftNode = std::make_shared<ScanPlanNode>(dbName,tableName,Names());


            } else if (leftexp->table_function) {

            } else {
                throw Exception("unknow table Expression");
            }

            if (rightexp->subquery) {

                rightNode = std::make_shared<UnionPlanNode>();
                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        rightexp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    auto subqueryRoot = analyse(selectQuery);
                    rightNode->addChild(subqueryRoot);
                    rightNode->setHeader(subqueryRoot->getHeader());
                }

            } else if (rightexp->database_and_table_name) {


                auto identifier = static_cast<const ASTIdentifier &>(*rightexp->database_and_table_name);
                std::string dbName ;
                std::string tableName ;
                if (!identifier.children.empty())
                {
                    if (identifier.children.size() != 2)
                        throw Exception("Qualified table name could have only two components", ErrorCodes::LOGICAL_ERROR);

                    dbName = typeid_cast<const ASTIdentifier &>(*identifier.children[0]).name;
                    tableName = typeid_cast<const ASTIdentifier &>(*identifier.children[1]).name;
                }


                std::set<std::string>  usedColumn;
                collectUsedColumns(query,usedColumn);

                rightNode = std::make_shared<ScanPlanNode>(dbName,tableName,usedColumn,queryToString(*right),context);
                //rightNode = std::make_shared<ScanPlanNode>(dbName,tableName,Names());

            } else if (rightexp->table_function) {

            } else {
                throw Exception("unknow table Expression");
            }

            ASTTableJoin * joininfo = typeid_cast<ASTTableJoin *>(right->table_join.get() );

            std::shared_ptr<PlanNode> joinNode = analyseJoin(leftNode, rightNode,joininfo);

            fromClauseNode->addChild(joinNode);
            fromClauseNode->setHeader(joinNode->getHeader());

        } else {
            throw new Exception("select query table child num err");
        }

        std::shared_ptr<PlanNode> afterPreWhere = analyseWhereClause(fromClauseNode, query);

        std::shared_ptr<PlanNode> afterAgg = analyseAggregate(afterPreWhere,query);

        //std::shared_ptr<PlanNode> afterPostAggWhere = analyseHavingClause(afterAgg,query);

        //std::shared_ptr<PlanNode> afterOrder = analyseOrderClause(afterPostAggWhere,query);

        //std::shared_ptr<PlanNode> afterLimit = analyseLimitClause(afterOrder,query);

        std::shared_ptr<PlanNode> aferSelectExp = analyseSelectExp(afterAgg,query);

        return  aferSelectExp;

    }
    std::shared_ptr<PlanNode> QueryAnalyzer::analyseJoin (std::shared_ptr<PlanNode> left ,std::shared_ptr<PlanNode> right,ASTTableJoin * joininfo) {


        Block leftHeader = left->getHeader();
        Block rightHeader = right->getHeader();

       // auto leftActions = std::make_shared<ExpressionActions>(leftHeader.getColumnsWithTypeAndName(), settings);
       // auto rightActions = std::make_shared<ExpressionActions>(rightHeader.getColumnsWithTypeAndName(), settings);
       // getRootActions(joininfo->using_expression_list, true, false, leftActions);
       // getRootActions(joininfo->using_expression_list, true, false, rightActions);

        auto & keys = typeid_cast<ASTExpressionList &>(*joininfo->using_expression_list);
        Names join_key;

        for (const auto & key : keys.children)
        {
            join_key.push_back(key->getColumnName());

        }
        std::string kind;
        std::string strictness;
        switch (joininfo->kind) {
            case ASTTableJoin::Kind::Comma:
                kind = "Comma";
                break;
            case ASTTableJoin::Kind::Cross:
                kind = "Cross";
                break;
            case ASTTableJoin::Kind::Full:
                kind = "Full";
                break;
            case ASTTableJoin::Kind::Inner:
                kind = "Inner";
                break;
            case ASTTableJoin::Kind::Left:
                kind = "Left";
                break;
            case ASTTableJoin::Kind::Right:
                kind = "Right";
                break;
            default:
                break;

        }
        switch (joininfo->strictness) {
            case ASTTableJoin::Strictness::All:
                strictness = "All";
                break;
            case ASTTableJoin::Strictness::Any:
                strictness = "Any";
                break;
            case ASTTableJoin::Strictness::Unspecified:
                strictness = "Unspecified";
                break;
            default:
                break;
        }
        std::shared_ptr<PlanNode> joinNode = std::make_shared<JoinPlanNode>(join_key,leftHeader,rightHeader,kind,strictness);

        joinNode->addChild(left);//left is child[0];
        joinNode->addChild(right);//right is child[1];

        return joinNode;
    }


    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseWhereClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        if(!query->where_expression)
            return child;

        Block header = child->getHeader();
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);
        getRootActions(query->where_expression, true, false, actions);

        std::shared_ptr<PlanNode> filterNode = std::make_shared<FilterPlanNode>(header,actions,query->where_expression->getColumnName());

        filterNode->addChild(child);
        return filterNode;
    }

    std::shared_ptr<PlanNode> QueryAnalyzer::analyseAggregate(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        std::shared_ptr<PlanNode> ret  ;
        Block header = child->getHeader(); // child input header
        NamesAndTypesList aggregation_keys;
        NamesAndTypesList aggregated_columns;
        AggregateDescriptions  aggregate_descriptions ;
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);

        bool has_aggregation = false;

        for (auto &astptr : query->select_expression_list->children) {

            const ASTFunction *node = typeid_cast<const ASTFunction *>(astptr.get());
            if (node && AggregateFunctionFactory::instance().isAggregateFunctionName(node->name)) {
                has_aggregation = true;
                AggregateDescription aggregate;
                aggregate.column_name = node->getColumnName();


                const ASTs &arguments = node->arguments->children;
                aggregate.arguments.resize(arguments.size());
                aggregate.argument_names.resize(arguments.size());
                DataTypes types(arguments.size());
                aggregate.argument_types.resize(arguments.size());

                for (size_t i = 0; i < arguments.size(); ++i) {
                    /// There can not be other aggregate functions within the aggregate functions.
                    //assertNoAggregates(arguments[i], "inside another aggregate function");
                    getRootActions(arguments[i], true, false, actions);
                    const std::string &name = arguments[i]->getColumnName();
                    types[i] = actions->getSampleBlock().getByName(name).type;
                    aggregate.arguments[i] = actions->getSampleBlock().getPositionByName(name);
                    aggregate.argument_names[i] = name;
                }
                aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters): Array();
                aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);
                aggregate.function_name = node->name;
                for(size_t i=0;i< types.size(); ++i){
                    aggregate.argument_types[i] = types[i]->getName();
                }



                aggregate_descriptions.push_back(aggregate);
            }
        }

        if(has_aggregation){
            if (query->group_expression_list){
                NameSet unique_keys;
                ASTs & group_asts = query->group_expression_list->children;
                for (ssize_t i = 0; i < ssize_t(group_asts.size()); ++i){
                    //ssize_t size = group_asts.size();
                    getRootActions(group_asts[i], true, false, actions);

                    const auto & column_name = group_asts[i]->getColumnName();
                    const auto & block = actions->getSampleBlock();

                    if (!block.has(column_name))
                        throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                    const auto & col = block.getByName(column_name);

                    /*
                    /// Constant expressions have non-null column pointer at this stage.
                    if (const auto is_constexpr = col.column){
                        /// But don't remove last key column if no aggregate functions, otherwise aggregation will not work.
                        if (!aggregate_descriptions.empty() || size > 1){
                            if (i + 1 < static_cast<ssize_t>(size))
                                group_asts[i] = std::move(group_asts.back());

                            group_asts.pop_back();

                            --i;
                            continue;
                        }
                    }
                     */

                    NameAndTypePair key{column_name, col.type};

                    /// Aggregation keys are uniqued.
                    if (!unique_keys.count(key.name)){
                        unique_keys.insert(key.name);
                        aggregation_keys.push_back(key);

                        /// Key is no longer needed, therefore we can save a little by moving it.
                        aggregated_columns.push_back(std::move(key));
                    }
                }

                if (group_asts.empty()){
                    query->group_expression_list = nullptr;
                    has_aggregation = query->having_expression || aggregate_descriptions.size();
                }
            }

            for (size_t i = 0; i < aggregate_descriptions.size(); ++i){
                AggregateDescription & desc = aggregate_descriptions[i];
                aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
            }

            //actions->execute()
            ret = std::make_shared<MergePlanNode>(header,aggregation_keys,aggregated_columns,aggregate_descriptions,context);
            auto aggNode = std::make_shared<AggPlanNode>(header, actions,aggregation_keys,aggregated_columns,aggregate_descriptions,context);

            aggNode->addChild(child);
            ret->addChild(aggNode);
        } else {
            ret = child ;
        }


        return ret ;
    }

    /*
    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseHavingClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        if(!query->having_expression)
            return  child;
        Block header = child->getHeader();// after  agg ,only agg key and agg cloumn
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);
        getRootActions(query->having_expression, true, false, actions);

        std::shared_ptr<PlanNode> filterNode = std::make_shared<FilterPlanNode>(header,actions,query->having_expression->getColumnName());

        filterNode->addChild(child);
        return filterNode;
    }

    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseOrderClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {
        return std::shared_ptr<PlanNode>();
    }

    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseLimitClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {
        return std::shared_ptr<PlanNode>();
    }
     */

    std::shared_ptr<PlanNode> QueryAnalyzer::analyseSelectExp(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        Block header = child->getHeader();// after  agg ,only agg key and agg cloumn
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);

        getRootActions(query->select_expression_list, true, false, actions);


        NamesWithAliases result_columns;

        ASTs asts = query->select_expression_list->children;
        for (size_t i = 0; i < asts.size(); ++i)
        {
            String result_name = asts[i]->getAliasOrColumnName(); // project to alias name
            //if (required_result_columns.empty() || required_result_columns.count(result_name))
            {
                result_columns.emplace_back(asts[i]->getColumnName(), result_name);

            }
        }
        actions->add(ExpressionAction::project(result_columns));
        auto projectPlanNode =  std::make_shared<ProjectPlanNode>(header, actions);
        projectPlanNode->addChild(child);
        return projectPlanNode;
    }


    void QueryAnalyzer::getRootActions(const ASTPtr &ast, bool no_subqueries, bool only_consts,
                                       ExpressionActionsPtr &actions) {
        ScopeStack scopes(actions, settings);
        getActionsImpl(ast, no_subqueries, only_consts, scopes);
        actions = scopes.popLevel();
    }

    void QueryAnalyzer::getActionsImpl(const ASTPtr &ast, bool no_subqueries, bool only_consts, ScopeStack &actions_stack) {
        /// If the result of the calculation already exists in the block.
        if ((typeid_cast<ASTFunction *>(ast.get()) || typeid_cast<ASTLiteral *>(ast.get()))
            && actions_stack.getSampleBlock().has(ast->getColumnName()))
            return;

        if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get())) {
            std::string name = node->getColumnName();
            if (!only_consts && !actions_stack.getSampleBlock().has(name)) {
                /// The requested column is not in the block.
                /// If such a column exists in the table, then the user probably forgot to surround it with an aggregate function or add it to GROUP BY.

                /*
                bool found = false;
                for (const auto &column_name_type : source_columns)
                    if (column_name_type.name == name)
                        found = true;

                if (found)
                    throw Exception("Column " + name + " is not under aggregate function and not in GROUP BY.",
                                    ErrorCodes::NOT_AN_AGGREGATE);
                */
            }
        } else if (ASTFunction *node = typeid_cast<ASTFunction *>(ast.get())) {
            if (node->name == "lambda")
                throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

            /// Function arrayJoin.
            if (node->name == "arrayJoin") {
                if (node->arguments->children.size() != 1)
                    throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);

                ASTPtr arg = node->arguments->children.at(0);
                getActionsImpl(arg, no_subqueries, only_consts, actions_stack);
                if (!only_consts) {
                    String result_name = node->getColumnName();
                    actions_stack.addAction(ExpressionAction::copyColumn(arg->getColumnName(), result_name));
                    NameSet joined_columns;
                    joined_columns.insert(result_name);
                    actions_stack.addAction(ExpressionAction::arrayJoin(joined_columns, false, *context));
                }

                return;
            }


            /// A special function `indexHint`. Everything that is inside it is not calculated
            /// (and is used only for index analysis, see KeyCondition).
            if (node->name == "indexHint") {
                actions_stack.addAction(ExpressionAction::addColumn(ColumnWithTypeAndName(
                        ColumnConst::create(ColumnUInt8::create(1, 1), 1), std::make_shared<DataTypeUInt8>(),
                        node->getColumnName())));
                return;
            }

            if (AggregateFunctionFactory::instance().isAggregateFunctionName(node->name))
                return;

            const FunctionBuilderPtr &function_builder = FunctionFactory::instance().get(node->name, *context);

            Names argument_names;
            DataTypes argument_types;
            bool arguments_present = true;

            /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
            //bool has_lambda_arguments = false;

            for (auto &child : node->arguments->children) {
                ASTFunction *lambda = typeid_cast<ASTFunction *>(child.get());
                if (lambda && lambda->name == "lambda") {
                    /// If the argument is a lambda expression, just remember its approximate type.
                    if (lambda->arguments->children.size() != 2)
                        throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

                    ASTFunction *lambda_args_tuple = typeid_cast<ASTFunction *>(
                            lambda->arguments->children.at(0).get());

                    if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
                        throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

                    //has_lambda_arguments = true;
                    argument_types.emplace_back(std::make_shared<DataTypeFunction>(
                            DataTypes(lambda_args_tuple->arguments->children.size())));
                    /// Select the name in the next cycle.
                    argument_names.emplace_back();

                } else {
                    /// If the argument is not a lambda expression, call it recursively and find out its type.
                    getActionsImpl(child, no_subqueries, only_consts, actions_stack);
                    std::string name = child->getColumnName();
                    if (actions_stack.getSampleBlock().has(name)) {
                        argument_types.push_back(actions_stack.getSampleBlock().getByName(name).type);
                        argument_names.push_back(name);
                    } else {
                        if (only_consts) {
                            arguments_present = false;
                        } else {
                            throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
                        }
                    }
                }
            }

            if (only_consts && !arguments_present)
                return;


            if (only_consts) {
                for (size_t i = 0; i < argument_names.size(); ++i) {
                    if (!actions_stack.getSampleBlock().has(argument_names[i])) {
                        arguments_present = false;
                        break;
                    }
                }
            }

            if (arguments_present)
                actions_stack.addAction(
                        ExpressionAction::applyFunction(function_builder, argument_names, node->getColumnName(),node->name));
        } else if (ASTLiteral * node = typeid_cast<ASTLiteral *>(ast.get())) {
            DataTypePtr type = applyVisitor(FieldToDataType(), node->value);

            ColumnWithTypeAndName column;
            column.column = type->createColumnConst(1, convertFieldToType(node->value, *type));
            column.type = type;
            column.name = node->getColumnName();

            actions_stack.addAction(ExpressionAction::addColumn(column));
        } else {
            for (auto &child : ast->children) {
                /// Do not go to FROM, JOIN, UNION.
                if (!typeid_cast<const ASTTableExpression *>(child.get())
                    && !typeid_cast<const ASTSelectQuery *>(child.get()))
                    getActionsImpl(child, no_subqueries, only_consts, actions_stack);
            }
        }
    }



    std::shared_ptr<PlanNode> QueryAnalyzer::addResultPlanNode(std::shared_ptr<PlanNode> plan){

        std::shared_ptr<PlanNode> resultPlanNode = std::make_shared<ResultPlanNode>();
        resultPlanNode->addChild(plan);
        return resultPlanNode;
    }

    
    void  QueryAnalyzer::normilizePlanTree(std::shared_ptr<PlanNode> root){ //remove fromClauseNode and unionNode with one child


           for(size_t i =0;i< root->getChilds().size();++i){

               auto child = root->getChild(i);
               if(typeid_cast<FromClauseNode*> (child.get())){
                   assert(child->getChilds().size() == 1);
                   auto fromClauseChild =  root->getChild(i)->getChild(0);
                   //fromClauseChild.use_count();
                   //root->getChild(i).use_count();
                   root->setChild(fromClauseChild,i);
                   //root->getChild(i).use_count();
               }
               if(typeid_cast<UnionPlanNode*> (child.get())){
                   if(child->getChilds().size() ==1 ){  // union node has only one child
                       auto UnionNodeChild = root->getChild(i)->getChild(0);
                       root->setChild(UnionNodeChild,i);
                   }
               }
           }
           for(size_t i =0;i< root->getChilds().size();++i){
               normilizePlanTree(root->getChild(i));
           }

    }

    void QueryAnalyzer::addExechangeNode(std::shared_ptr<PlanNode> root ){ //from bottom to top

        // agg -> agg + merge
        // join -> shuffle join  , broadcast  join


        for(std::shared_ptr<PlanNode> child : root->getChilds()){
            addExechangeNode(child);
        }

        if( ScanPlanNode* scanPlanNode = typeid_cast<ScanPlanNode*> (root.get())){
            //get distribution from static module

            scanPlanNode->buildDistributionAndScanSource(); //only partitionKey and num basc info

        } else if( JoinPlanNode* joinPlanNode = typeid_cast<JoinPlanNode*>(root.get())){

           assert(joinPlanNode->getChilds().size() ==2 );

           //todo broadcast case

            std::shared_ptr<Distribution>  joinNodeDistribution ;

           if(joinPlanNode->getChild(0)->getDistribution()->equals(*(joinPlanNode->getChild(1)->getDistribution())) &&
                   joinPlanNode->getChild(0)->getDistribution()->keyEquals(joinPlanNode->joinKeys)){

               auto leftDis = joinPlanNode->getChild(0)->getDistribution();
               joinNodeDistribution =  std::make_shared<Distribution>(leftDis->distributeKeys,leftDis->parititionIds);

               std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::tone2onejoin,joinNodeDistribution); // exechangeNode sender distribution,
               if( joinPlanNode->getChild(0)->exechangeCost() > joinPlanNode->getChild(1)->exechangeCost()){                                       // submit father stage will set executor info in distribution for child
                   enode->addChild(joinPlanNode->getChild(1));
                   joinPlanNode->setChild(enode,1); // 0 is origin table ,1 is enode
                   joinPlanNode->setHashTable("right");
                } else{
                   /*
                   enode->addChild(joinPlanNode->getChild(0));
                   joinPlanNode->setChild(enode,0);
                   joinPlanNode->setHashTable("left");
                   */

                   enode->addChild(joinPlanNode->getChild(1));
                   joinPlanNode->setChild(enode,1); // 0 is origin table ,1 is enode
                   joinPlanNode->setHashTable("right");
                }



           }else if( joinPlanNode->getChild(0)->getDistribution()->keyEquals(joinPlanNode->joinKeys)){

               auto leftDis = joinPlanNode->getChild(0)->getDistribution();
               joinNodeDistribution =  std::make_shared<Distribution>(leftDis->distributeKeys,leftDis->parititionIds);

               std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::toneshufflejoin,joinNodeDistribution); // exechangeNode sender distribution
               enode->addChild(joinPlanNode->getChild(1));                                                                                  // submit father stage will set executor info in distribution for child

               joinPlanNode->setChild(enode,1);
               joinPlanNode->setHashTable("right");


           }
           /*
           else if( joinPlanNode->getChild(1)->getDistribution()->keyEquals(joinPlanNode->joinKeys)){


               auto rightDis = joinPlanNode->getChild(1)->getDistribution();
               joinNodeDistribution =  std::make_shared<ExechangeDistribution>(rightDis->distributeKeys,rightDis->partitionNum);

               std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::toneshufflejoin,joinNodeDistribution);
               enode->addChild(joinPlanNode->getChild(0)); //add child1 distribution ,child 0 know how to redistribute

               joinPlanNode->setChild(enode,0);
               joinPlanNode->setHashTable("left");

           }*/
           else {

               joinNodeDistribution = std::make_shared<Distribution>(joinPlanNode->joinKeys,64);

               std::shared_ptr<ExechangeNode> enode0 = std::make_shared<ExechangeNode>(DataExechangeType::ttwoshufflejoin,joinNodeDistribution);
               enode0->addChild(joinPlanNode->getChild(0));
               enode0->addChild(joinPlanNode->getChild(1));

               joinPlanNode->cleanAndResizeToNChild(1);
               joinPlanNode->setChild(enode0,0);
               joinPlanNode->setHashTable("right");// todo  choose maller table

           }

            joinPlanNode->setDistribution(joinNodeDistribution);


        } else if(MergePlanNode* mergePlanNode = typeid_cast<MergePlanNode*>(root.get())){

            (void)mergePlanNode;
            std::shared_ptr<Distribution> distribution = std::make_shared<Distribution>();
            AggPlanNode* aggPlanNode = typeid_cast<AggPlanNode*>(root->getChild(0).get());
            assert(aggPlanNode != NULL);

            if(root->getChild(0)->getDistribution()->parititionIds.size() > 1){  //need to merge

                distribution->parititionIds.push_back(0);
                distribution->distributeKeys = std::vector<std::string>();

                std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::taggmerge,distribution);// submit stage will set executor info in distribution
                enode->addChild(root->getChild(0));

                root->setChild(enode,0);
                root->setDistribution(distribution);

            } else {

                aggPlanNode->setFinal(true);
                root->setDistribution(aggPlanNode->getDistribution());     // need to delete later;
            }



        } else if(UnionPlanNode* unionPlanNode = typeid_cast<UnionPlanNode*>(root.get())){

            (void )unionPlanNode;
            std::shared_ptr<Distribution> distribution = std::make_shared<Distribution>();
            std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::tunion,distribution); // submit stage will set executor info in distribution


            distribution->parititionIds.push_back(0);
            distribution->distributeKeys = std::vector<std::string>();
            for (auto child : root->getChilds()) {
                enode->addChild(child);
            }
            root->cleanAndResizeToNChild(1);

            root->setChild(enode,0);


            root->setDistribution(distribution);

        }
        /*else if(ResultPlanNode * resultPlanNode = typeid_cast<ResultPlanNode *>(root.get())){ // as the exechange node

               std::shared_ptr<Distribution> distribution = std::make_shared<ExechangeDistribution>();

               distribution->partitionNum = 1;
               distribution->distributeKeys = std::vector<std::string>();
               resultPlanNode->setDistribution(distribution);


        } */
        else {

            assert(root->getChilds().size() ==1 );
            root->setDistribution(root->getChild(0)->getDistribution());
        }


    }

    void QueryAnalyzer::removeUnusedMergePlanNode(std::shared_ptr<PlanNode> root){


        for(size_t i=0;i < root->getChilds().size() ;++i){

            removeUnusedMergePlanNode(root->getChild(i));
            MergePlanNode * mergePlanNode = typeid_cast<MergePlanNode*>(root->getChild(i).get());
            if(mergePlanNode){
                ExechangeNode * exechangeNode = typeid_cast<ExechangeNode*>(mergePlanNode->getChild(0).get());
                if(!exechangeNode){
                    auto aggPlanNode = mergePlanNode->getChild(0);
                    root->setChild(aggPlanNode,i); //  child 0 is agg node
                }
            }
        }

    }


    void QueryAnalyzer::splitStageByExechangeNode(std::shared_ptr<PlanNode> root, std::shared_ptr<Stage>  currentStage){

        if( JoinPlanNode* joinPlanNode = typeid_cast<JoinPlanNode*>(root.get())){

            if(joinPlanNode->getChilds().size() == 1){ // child is exechange node , exechange node has two child

                currentStage->addPlanNode(root);
                auto enode = joinPlanNode->getChild(0);
                assert( typeid_cast<ExechangeNode *>(enode.get()));
                auto etype = (typeid_cast<ExechangeNode *>(enode.get()))->getDateExechangeType();
                assert( etype == DataExechangeType::ttwoshufflejoin);
                currentStage->setSourceExechangeType(etype);
                currentStage->hasExechange = true;
                currentStage->setDistribution(enode->getDistribution());

                assert(enode->getChilds().size() == 2 ) ; // 0 is left ,1 is right

                auto lstage = std::make_shared<Stage>(jobId,stageid++,context);
                splitStageByExechangeNode(enode->getChild(0),lstage);
                currentStage->addChild(lstage->stageId,lstage);
                currentStage->addMainTableChildStageId(lstage->stageId);
                lstage->setFather(currentStage);
                lstage->setDestExechangeType(etype);


                auto rstage = std::make_shared<Stage>(jobId,stageid++,context);
                splitStageByExechangeNode(enode->getChild(1),rstage);
                currentStage->addChild(rstage->stageId,rstage);
                currentStage->addRightTableChildStageId(rstage->stageId);
                joinPlanNode->setHashTableStageId(rstage->stageId);
                rstage->setFather(currentStage);
                rstage->setDestExechangeType(etype);



            }else {
                ExechangeNode * left = typeid_cast<ExechangeNode *>(joinPlanNode->getChild(0).get());
                ExechangeNode * right = typeid_cast<ExechangeNode *>(joinPlanNode->getChild(1).get());

                if(left) {

                    currentStage->addPlanNode(root);
                    currentStage->setSourceExechangeType(left->getDateExechangeType());
                    currentStage->hasExechange = true;
                    currentStage->setDistribution(left->getDistribution());

                    assert(left->getChilds().size() == 1);
                    auto childStage = std::make_shared<Stage>(jobId,stageid++,context);
                    splitStageByExechangeNode(left->getChild(0),childStage);
                    currentStage->addChild(childStage->stageId,childStage);
                    currentStage->addRightTableChildStageId(childStage->stageId);
                    joinPlanNode->setHashTableStageId(childStage->stageId);
                    childStage->setDestExechangeType(left->getDateExechangeType());
                    childStage->setFather(currentStage);

                    splitStageByExechangeNode(joinPlanNode->getChild(1),currentStage);// right child is not exechangeNode,currentStage go throw this path


                } else if(right){

                    currentStage->addPlanNode(root);
                    currentStage->setSourceExechangeType(right->getDateExechangeType());
                    currentStage->hasExechange = true;
                    currentStage->setDistribution(right->getDistribution());

                    assert(right->getChilds().size() == 1);
                    auto childStage = std::make_shared<Stage>(jobId,stageid++,context);
                    splitStageByExechangeNode(right->getChild(0),childStage);
                    currentStage->addChild(childStage->stageId,childStage);
                    currentStage->addRightTableChildStageId(childStage->stageId);
                    joinPlanNode->setHashTableStageId(childStage->stageId);
                    childStage->setDestExechangeType(right->getDateExechangeType());
                    childStage->setFather(currentStage);

                    splitStageByExechangeNode(joinPlanNode->getChild(0),currentStage);// left child is not exechangeNode,currentStage go throw this path


                } else {
                    throw  Exception("join child with no exechange node");
                }

            }

        } else if(MergePlanNode* mergePlanNode = typeid_cast<MergePlanNode*>(root.get())){

            ExechangeNode* enode = typeid_cast<ExechangeNode*> (mergePlanNode->getChild(0).get());
            assert(enode != NULL);

            currentStage->addPlanNode(root);
            currentStage->setSourceExechangeType(enode->getDateExechangeType());
            currentStage->hasExechange = true;
            currentStage->setDistribution(enode->getDistribution());
            auto childStage =  std::make_shared<Stage>(jobId,stageid++,context);

            assert(enode->getChilds().size() == 1); // aggPlanNode
            splitStageByExechangeNode(enode->getChild(0),childStage);

            currentStage->addChild(childStage->stageId,childStage);
            currentStage->addMainTableChildStageId(childStage->stageId);
            childStage->setDestExechangeType(enode->getDateExechangeType());
            childStage->setFather(currentStage);

        } else if(UnionPlanNode* unionPlanNode = typeid_cast<UnionPlanNode*>(root.get())){


            ExechangeNode* enode = typeid_cast<ExechangeNode*> (unionPlanNode->getChild(0).get());
            assert(enode != NULL);
            currentStage->addPlanNode(root);
            currentStage->setSourceExechangeType(enode->getDateExechangeType());
            currentStage->hasExechange = true;
            currentStage->setDistribution(enode->getDistribution());

            std::vector<std::shared_ptr<PlanNode>> childs = enode->getChilds();

            //assert(childs.size() > 1);
            for(size_t i=0;i< childs.size() ;++i){

                auto childStage =  std::make_shared<Stage>(jobId,stageid++,context);
                splitStageByExechangeNode(childs[i],childStage);
                currentStage->addChild(childStage->stageId,childStage);
                currentStage->addMainTableChildStageId(childStage->stageId);
                childStage->setDestExechangeType(enode->getDateExechangeType());
                childStage->setFather(currentStage);
            }


        } else if(ScanPlanNode* scanPlanNode = typeid_cast<ScanPlanNode*>(root.get())) { // bottom , stop recursive

            currentStage->addPlanNode(root);
            currentStage->setDistribution(scanPlanNode->getDistribution());
            currentStage->hasScan = true;
            currentStage->scanSource  = scanPlanNode->scanSource;

        } else if(ResultPlanNode* resultPlanNode = typeid_cast<ResultPlanNode*>(root.get())){ // top
            (void)resultPlanNode;
            currentStage->addPlanNode(root);
            std::shared_ptr<Distribution> resultDistribution = std::make_shared<Distribution>();
            resultDistribution->parititionIds.push_back(0);
            currentStage->setDistribution(resultDistribution);
            currentStage->isResultStage_ = true;
            splitStageByExechangeNode(root->getChild(0),currentStage);
        } else if(!typeid_cast<ExechangeNode*>(root.get())){
            assert(root->getChilds().size() == 1);
            currentStage->addPlanNode(root);
            splitStageByExechangeNode(root->getChild(0),currentStage);
        } else {
            throw Exception("unexpected node path");
        }
    }

    void  QueryAnalyzer::collectUsedColumns( IAST * ast ,std::set<std::string> & usedColumn){



        if (ASTIdentifier * node = typeid_cast< ASTIdentifier *>(ast))
        {
            if (node->kind == ASTIdentifier::Column)
            {
                usedColumn.insert(node->name);
            }

            return;
        }

        for (auto & child : ast->children)
        {
            /** We will not go to the ARRAY JOIN section, because we need to look at the names of non-ARRAY-JOIN columns.
              * There, `collectUsedColumns` will send us separately.
              */
            if (!typeid_cast<const ASTSelectQuery *>(child.get())
                && !typeid_cast<const ASTArrayJoin *>(child.get())
                && !typeid_cast<const ASTTableExpression *>(child.get()))
                collectUsedColumns(child.get(), usedColumn);
        }

    }

    bool QueryAnalyzer::onlyOneStage(std::shared_ptr<PlanNode> root){
        assert(root->getName() == "resultPlanNode");
        auto cur  = root;
        while(cur){

            ScanPlanNode * scanPlanNode = typeid_cast<ScanPlanNode *>(cur.get());
            ExechangeNode * exechangeNode = typeid_cast<ExechangeNode *>(cur.get());
            JoinPlanNode * joinPlanNode = typeid_cast<JoinPlanNode *>(cur.get());
            if(scanPlanNode)
                return true;
            else if(joinPlanNode){
                if(joinPlanNode->getChilds().size() == 2 ){

                    ExechangeNode * left = typeid_cast<ExechangeNode *>(joinPlanNode->getChild(0).get());
                    if(!left){
                        cur = joinPlanNode->getChild(0);
                    } else{
                        cur = joinPlanNode->getChild(1);
                    }

                } else {
                    ExechangeNode * exechangeNode = typeid_cast<ExechangeNode *>(joinPlanNode->getChild(0).get());
                    assert(exechangeNode != NULL);
                    return false;
                }
            }
            else if(exechangeNode)
                return false;
            else if(cur->getChilds().size() != 1)
                return false;
            else
                cur = cur->getChild(0);

        }
        return false;

    }
    void QueryAnalyzer::addUnionToSplitStage(std::shared_ptr<PlanNode> root){

        assert(root->getName() == "resultPlanNode");

        assert(root->getChilds().size() == 1);

        auto child = root->getChild(0);


        auto unionPlanNode = std::make_shared<UnionPlanNode>();
        unionPlanNode->setHeader(child->getHeader());

        std::shared_ptr<Distribution> distribution = std::make_shared<Distribution>();
        std::shared_ptr<ExechangeNode> enode = std::make_shared<ExechangeNode>(DataExechangeType::tunion,distribution); // submit stage will set executor info in distribution
        distribution->parititionIds.push_back(0) ;
        distribution->distributeKeys = std::vector<std::string>();

        enode->addChild(child);
        unionPlanNode->addChild(enode);

        //root->cleanChild();
        root->setChild(unionPlanNode,0);

        unionPlanNode->setDistribution(distribution);
        root->setDistribution(distribution);



    }

    /*

    void QueryAnalyzer::submitStage(std::shared_ptr<Stage> root){

        //first submit father ;
        //then father stage distribution thus exechanage node receiver distribution is set
        //father stage exechange node receiver is the same with child stage exechange node sender
        //then submit child stage, child know where to send data by exechange node sender

        // stage bottom node is exechange node receiver
        // stage know his distribution, exechange receiver -> mergePlanNode ,
        // exechange receiver-> UnionPlanNode , exechange receiver-> JoinPlanNode , ScanPlanNode
        // if father stage submited and they set their exechange receiver and exechange receiver helper distribution
        // child stage' exechange sender know his distribution ,and sender know where to send

        // child                                          ==>  father
        //exechanage receiver -> ... -> exechange sender  ==>  exechanage receiver -> ... -> exechange sender
        //set stage father's exechanage receiver distribution info  will effect
        //stage child's exechange sender distribution info
        //all distribution ptr is created in  addExechangeNode
    }
    */


}