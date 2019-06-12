//
// Created by jungle on 19-6-10.
//
#include <Interpreters/QueryAnalyzer.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/PlanNode/JoinNode.h>

namespace DB {


std::shared_ptr<PlanNode> QueryAnalyzer::analyse(DB::ASTSelectQuery *query) {

    size_t  cnum  = query->tables->children.size();

    if(cnum == 1){


    } else if(cnum == 2){

        std::shared_ptr<JoinNode> joinNode = std::make_shared<JoinNode>();
        std::shared_ptr<UnionNode> leftUnionNode = std::make_shared<UnionNode>();
        std::shared_ptr<UnionNode> rightUnionNode = std::make_shared<UnionNode>();

        ASTTablesInSelectQueryElement *  left = typeid_cast<ASTTablesInSelectQueryElement *> (query->tables->children[0].get());
        ASTTablesInSelectQueryElement *  right = typeid_cast<ASTTablesInSelectQueryElement *> (query->tables->children[1].get());
        ASTTableExpression  * leftexp =  typeid_cast<ASTTableExpression *> ( left->table_expression.get());
        ASTTableExpression  * rightexp =  typeid_cast<ASTTableExpression *> ( right->table_expression.get());
        if(leftexp->subquery){

            ASTSelectWithUnionQuery * unionQuery = typeid_cast<ASTSelectWithUnionQuery *> (leftexp->subquery->children[0].get());
            for (size_t i = 0 ; i < unionQuery->list_of_selects->children.size() ; ++i){
                ASTSelectQuery  * selectQuery = typeid_cast<ASTSelectQuery *> (unionQuery->list_of_selects->children[i].get());
                leftUnionNode.add(analyse(selectQuery));
            }
        }else if(leftexp->database_and_table_name)  {

        }else if(leftexp->table_function){

        } else {

            throw  Exception ("unknow table Expression");
        }

    } else {

        throw new Exception("select query table child num err");
    }

}




}