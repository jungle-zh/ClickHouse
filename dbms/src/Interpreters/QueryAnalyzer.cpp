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
#include <Interpreters/PlanNode/JoinNode.h>
#include <Interpreters/PlanNode/UnionNode.h>
#include <Interpreters/PlanNode/FromClauseNode.h>
#include <Interpreters/PlanNode/FilterNode.h>
#include <Interpreters/AggregateDescription.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Parsers/ASTIdentifier.h>
#include <DataTypes/DataTypeFunction.h>



namespace DB {


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

                childNode = std::make_shared<UnionNode>();

                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        exp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    childNode->add(analyse(selectQuery));
                }


            } else if (exp->database_and_table_name) {

            } else if (exp->table_function) {

            } else {
                throw Exception("unknow table Expression");
            }

            fromClauseNode->add(childNode);


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

                leftNode = std::make_shared<UnionNode>();
                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        leftexp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    leftNode.add(analyse(selectQuery));
                }

            } else if (leftexp->database_and_table_name) {

            } else if (leftexp->table_function) {

            } else {
                throw Exception("unknow table Expression");
            }

            if (rightexp->subquery) {

                rightNode = std::make_shared<UnionNode>();
                ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(
                        rightexp->subquery->children[0].get());
                for (size_t i = 0; i < unionQuery->list_of_selects->children.size(); ++i) {
                    ASTSelectQuery *selectQuery = typeid_cast<ASTSelectQuery *>(
                            unionQuery->list_of_selects->children[i].get());
                    rightNode.add(analyse(selectQuery));
                }

            } else if (rightexp->database_and_table_name) {

            } else if (rightexp->table_function) {

            } else {

                throw Exception("unknow table Expression");
            }


            std::shared_ptr<JoinNode> joinNode = analyseJoin(leftNode, rightNode,query);


            fromClauseNode->add(joinNode);


        } else {

            throw new Exception("select query table child num err");
        }

        std::shared_ptr<PlanNode> afterPreWhere = analyseWhereClause(fromClauseNode, query);

        std::shared_ptr<PlanNode> afterAgg = analyseAggregate(afterPreWhere,query);

        std::shared_ptr<PlanNode> afterPostAggWhere = analyseHavingClause(afterAgg,query);

        std::shared_ptr<PlanNode> afterOrder = analyseOrderClause(afterPostAggWhere,query);

        std::shared_ptr<PlanNode> afterLimit = analyseLimitClause(afterOrder,query);

        std::shared_ptr<PlanNode> aferSelectExp = analyseSelectExp(afterLimit,query);

    }
    std::shared_ptr<PlanNode>
    analyseJoin (std::shared_ptr<PlanNode> left ,std::shared_ptr<PlanNode> right,ASTSelectQuery * query) {

         left->getHeader();

    }


    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseWhereClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        if(!query->where_expression)
            return child;

        Block header = child->getHeader();
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);
        getRootActions(query->where_expression, true, false, actions);

        std::shared_ptr<PlanNode> filterNode = std::make_shared<FilterNode>(header,actions,query->where_expression->getColumnName());

        filterNode->add(child);
        return filterNode;
    }

    std::shared_ptr<PlanNode> QueryAnalyzer::analyseAggregate(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        std::shared_ptr<PlanNode> ret  ;
        Block header = child->getHeader();
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
                aggregate.argument_names.resize(arguments.size());
                DataTypes types(arguments.size());

                for (size_t i = 0; i < arguments.size(); ++i) {
                    /// There can not be other aggregate functions within the aggregate functions.
                    assertNoAggregates(arguments[i], "inside another aggregate function");
                    getRootActions(arguments[i], true, false, actions);
                    const std::string &name = arguments[i]->getColumnName();
                    types[i] = actions->getSampleBlock().getByName(name).type;
                    aggregate.argument_names[i] = name;
                }
                aggregate.parameters = (node->parameters) ? getAggregateFunctionParametersArray(node->parameters): Array();
                aggregate.function = AggregateFunctionFactory::instance().get(node->name, types, aggregate.parameters);

                aggregate_descriptions.push_back(aggregate);
            }
        }

        if(has_aggregation){
            if (query->group_expression_list){
                NameSet unique_keys;
                ASTs & group_asts = query->group_expression_list->children;
                for (ssize_t i = 0; i < ssize_t(group_asts.size()); ++i){
                    ssize_t size = group_asts.size();
                    getRootActions(group_asts[i], true, false, actions);

                    const auto & column_name = group_asts[i]->getColumnName();
                    const auto & block = actions->getSampleBlock();

                    if (!block.has(column_name))
                        throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

                    const auto & col = block.getByName(column_name);

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

            ret = std::make_shared<AggNode>(header, actions,aggregation_keys,aggregated_columns,aggregate_descriptions);
            ret->add(child);
        } else {
            ret = child ;
        }


        return ret ;
    }

    std::shared_ptr<PlanNode>
    QueryAnalyzer::analyseHavingClause(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {

        if(!query->having_expression)
            return  child;
        Block header = child->getHeader();// after  agg ,only agg key and agg cloumn
        auto actions = std::make_shared<ExpressionActions>(header.getColumnsWithTypeAndName(), settings);
        getRootActions(query->having_expression, true, false, actions);

        std::shared_ptr<PlanNode> filterNode = std::make_shared<FilterNode>(header,actions,query->having_expression->getColumnName());

        filterNode->add(child);
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

    std::shared_ptr<PlanNode> QueryAnalyzer::analyseSelectExp(std::shared_ptr<PlanNode> child, ASTSelectQuery *query) {


        return std::shared_ptr<PlanNode>();
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

                bool found = false;
                for (const auto &column_name_type : source_columns)
                    if (column_name_type.name == name)
                        found = true;

                if (found)
                    throw Exception("Column " + name + " is not under aggregate function and not in GROUP BY.",
                                    ErrorCodes::NOT_AN_AGGREGATE);
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
                    actions_stack.addAction(ExpressionAction::arrayJoin(joined_columns, false, context));
                }

                return;
            }

            if (functionIsInOrGlobalInOperator(node->name)) {
                if (!no_subqueries) {
                    /// Let's find the type of the first argument (then getActionsImpl will be called again and will not affect anything).
                    getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);

                    /// Transform tuple or subquery into a set.
                    makeSet(node, actions_stack.getSampleBlock());
                } else {
                    if (!only_consts) {
                        /// We are in the part of the tree that we are not going to compute. You just need to define types.
                        /// Do not subquery and create sets. We insert an arbitrary column of the correct type.
                        ColumnWithTypeAndName fake_column;
                        fake_column.name = node->getColumnName();
                        fake_column.type = std::make_shared<DataTypeUInt8>();
                        actions_stack.addAction(ExpressionAction::addColumn(fake_column));
                        getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);
                    }
                    return;
                }
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

            const FunctionBuilderPtr &function_builder = FunctionFactory::instance().get(node->name, context);

            Names argument_names;
            DataTypes argument_types;
            bool arguments_present = true;

            /// If the function has an argument-lambda expression, you need to determine its type before the recursive call.
            bool has_lambda_arguments = false;

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

                    has_lambda_arguments = true;
                    argument_types.emplace_back(std::make_shared<DataTypeFunction>(
                            DataTypes(lambda_args_tuple->arguments->children.size())));
                    /// Select the name in the next cycle.
                    argument_names.emplace_back();
                } else if (prepared_sets.count(child.get())) {
                    ColumnWithTypeAndName column;
                    column.type = std::make_shared<DataTypeSet>();

                    const SetPtr &set = prepared_sets[child.get()];

                    /// If the argument is a set given by an enumeration of values (so, the set was already built), give it a unique name,
                    ///  so that sets with the same literal representation do not fuse together (they can have different types).
                    if (!set->empty())
                        column.name = getUniqueName(actions_stack.getSampleBlock(), "__set");
                    else
                        column.name = child->getColumnName();

                    if (!actions_stack.getSampleBlock().has(column.name)) {
                        column.column = ColumnSet::create(1, set);

                        actions_stack.addAction(ExpressionAction::addColumn(column));
                    }

                    argument_types.push_back(column.type);
                    argument_names.push_back(column.name);
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

            if (has_lambda_arguments && !only_consts) {
                function_builder->getLambdaArgumentTypes(argument_types);

                /// Call recursively for lambda expressions.
                for (size_t i = 0; i < node->arguments->children.size(); ++i) {
                    ASTPtr child = node->arguments->children[i];

                    ASTFunction *lambda = typeid_cast<ASTFunction *>(child.get());
                    if (lambda && lambda->name == "lambda") {
                        const DataTypeFunction *lambda_type = typeid_cast<const DataTypeFunction *>(
                                argument_types[i].get());
                        ASTFunction *lambda_args_tuple = typeid_cast<ASTFunction *>(
                                lambda->arguments->children.at(0).get());
                        ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
                        NamesAndTypesList lambda_arguments;

                        for (size_t j = 0; j < lambda_arg_asts.size(); ++j) {
                            ASTIdentifier *identifier = typeid_cast<ASTIdentifier *>(lambda_arg_asts[j].get());
                            if (!identifier)
                                throw Exception("lambda argument declarations must be identifiers",
                                                ErrorCodes::TYPE_MISMATCH);

                            String arg_name = identifier->name;

                            lambda_arguments.emplace_back(arg_name, lambda_type->getArgumentTypes()[j]);
                        }

                        actions_stack.pushLevel(lambda_arguments);
                        getActionsImpl(lambda->arguments->children.at(1), no_subqueries, only_consts, actions_stack);
                        ExpressionActionsPtr lambda_actions = actions_stack.popLevel();

                        String result_name = lambda->arguments->children.at(1)->getColumnName();
                        lambda_actions->finalize(Names(1, result_name));
                        DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;

                        Names captured;
                        Names required = lambda_actions->getRequiredColumns();
                        for (size_t j = 0; j < required.size(); ++j)
                            if (findColumn(required[j], lambda_arguments) == lambda_arguments.end())
                                captured.push_back(required[j]);

                        /// We can not name `getColumnName()`,
                        ///  because it does not uniquely define the expression (the types of arguments can be different).
                        String lambda_name = getUniqueName(actions_stack.getSampleBlock(), "__lambda");

                        auto function_capture = std::make_shared<FunctionCapture>(
                                lambda_actions, captured, lambda_arguments, result_type, result_name);
                        actions_stack.addAction(
                                ExpressionAction::applyFunction(function_capture, captured, lambda_name));

                        argument_types[i] = std::make_shared<DataTypeFunction>(lambda_type->getArgumentTypes(),
                                                                               result_type);
                        argument_names[i] = lambda_name;
                    }
                }
            }

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
                        ExpressionAction::applyFunction(function_builder, argument_names, node->getColumnName()));
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


}