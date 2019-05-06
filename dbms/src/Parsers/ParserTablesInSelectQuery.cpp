#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <stack>
#include <common/logger_useful.h>
#include <iostream>
#include <Common/typeid_cast.h>
#include "queryToString.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


bool ParserTableExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTTableExpression>();

    if (!ParserWithOptionalAlias(std::make_unique<ParserSubquery>(), true).parse(pos, res->subquery, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserFunction>(), true).parse(pos, res->table_function, expected)
        && !ParserWithOptionalAlias(std::make_unique<ParserCompoundIdentifier>(), true).parse(pos, res->database_and_table_name, expected))
        return false;

    /// FINAL
    if (ParserKeyword("FINAL").ignore(pos, expected))
        res->final = true;

    /// SAMPLE number
    if (ParserKeyword("SAMPLE").ignore(pos, expected))
    {
        ParserSampleRatio ratio;

        if (!ratio.parse(pos, res->sample_size, expected))
            return false;

        /// OFFSET number
        if (ParserKeyword("OFFSET").ignore(pos, expected))
        {
            if (!ratio.parse(pos, res->sample_offset, expected))
                return false;
        }
    }

    if (res->database_and_table_name)
        res->children.emplace_back(res->database_and_table_name);
    if (res->table_function)
        res->children.emplace_back(res->table_function);
    if (res->subquery)
        res->children.emplace_back(res->subquery);
    if (res->sample_size)
        res->children.emplace_back(res->sample_size);
    if (res->sample_offset)
        res->children.emplace_back(res->sample_offset);

    node = res;
    return true;
}


bool ParserArrayJoin::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto res = std::make_shared<ASTArrayJoin>();

    /// [LEFT] ARRAY JOIN expr list
    Pos saved_pos = pos;
    bool has_array_join = false;

    if (ParserKeyword("LEFT ARRAY JOIN").ignore(pos, expected))
    {
        res->kind = ASTArrayJoin::Kind::Left;
        has_array_join = true;
    }
    else
    {
        pos = saved_pos;

        /// INNER may be specified explicitly, otherwise it is assumed as default.
        ParserKeyword("INNER").ignore(pos, expected);

        if (ParserKeyword("ARRAY JOIN").ignore(pos, expected))
        {
            res->kind = ASTArrayJoin::Kind::Inner;
            has_array_join = true;
        }
    }

    if (!has_array_join)
        return false;

    if (!ParserExpressionList(false).parse(pos, res->expression_list, expected))
        return false;

    if (res->expression_list)
        res->children.emplace_back(res->expression_list);

    node = res;
    return true;
}



bool ParserTablesInSelectQueryElement::parseEnhanceJoinImpl1(DB::IParser::Pos &pos, DB::ASTPtr &node, DB::Expected &expected) {

    //  From
    //  has join ?
    //  ( ) join () using
    //   t  join () using
    //  ( ) join  t using

    //  subquery
    //  ( )
    //   t

    //  array join

    Pos  tmpPos1 = pos;

    std::string pos_info = "";
    while (tmpPos1.isValid()){
        pos_info += tmpPos1.get().debug_str() ;
        pos_info += "#";
        ++ tmpPos1;
    }
    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"pos info : " + pos_info);
    //std::cerr << "pos info :" << pos_info ;
    bool  ret = true;
    auto res = std::make_shared<ASTTablesInSelectQueryElement>();
    if(is_first){

        Pos   tmpPos = pos;
        std::stack<Token> st ;


        auto table_join = std::make_shared<ASTTableEnhanceJoin>();
        if(tmpPos.get().type ==  TokenType::OpeningRoundBracket){


            LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"pos is :" + tmpPos.get().debug_str() ) ;
            //std::cerr << "pos is " << pos_info ;
            st.push(tmpPos.get());

            while (st.size() != 0 &&  tmpPos.isValid()) {
                ++tmpPos;
                if (tmpPos.get().type == TokenType::OpeningRoundBracket) {
                    st.push(tmpPos.get());
                } else if (tmpPos.get().type == TokenType::ClosingRoundBracket) {
                    st.pop();
                }
            }

            if( !tmpPos.isValid()){
                throw  Exception("not find ClosingRoundBracket") ;
            }

            if (st.size() == 0) {
                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"pos is :" + tmpPos.get().debug_str() ) ;
                ++tmpPos; //skip the ClosingRoundBracket
                if (parseJoinInfo(tmpPos, expected, table_join)) {  // join


                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1 left table pos is :" + pos.get().debug_str() ) ;
                    ret = ret & ParserTableExpression().parse(pos, table_join->left_table_expression, expected);

                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2  left table pos is :" + pos.get().debug_str() ) ;
                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1 right table pos is :" + tmpPos.get().debug_str() ) ;

                    ret = ret & ParserTableExpression().parse(tmpPos, table_join->right_table_expression, expected);

                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2 right table pos is :" + tmpPos.get().debug_str() ) ;

                    pos = tmpPos;
                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1 before using pos is :" + pos.get().debug_str() ) ;
                    ret = ret & parseUsing(pos,table_join,expected);

                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2 before using pos is :" + pos.get().debug_str() ) ;


                    if(table_join->left_table_expression){
                        table_join->children.emplace_back(table_join->left_table_expression);
                    }

                    if(table_join->right_table_expression){
                        table_join->children.emplace_back(table_join->right_table_expression);
                    }


                    res->table_join = table_join;


                } else {  //sub query

                    ret =  ParserTableExpression().parse(pos, res->table_expression, expected);

                }

            }
        } else {
            LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"pos is :" + tmpPos.get().debug_str() ) ;
            ++tmpPos;   //skip the table name
            if (parseJoinInfo(tmpPos, expected, table_join)) {

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1 left table pos is :" + pos.get().debug_str() ) ;

                ret  = ret & ParserTableExpression().parse(pos, table_join->left_table_expression, expected);

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2 left table pos is :" + pos.get().debug_str() ) ;

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1 right table pos is :" + tmpPos.get().debug_str() ) ;
                ret  = ret & ParserTableExpression().parse(tmpPos, table_join->right_table_expression, expected);

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2 right table pos is :" + tmpPos.get().debug_str() ) ;

                if( tmpPos.get().type == TokenType::ClosingRoundBracket){
                    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"skip ')'");
                     ++ tmpPos;
                }
                pos = tmpPos;

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"1  using pos is :" + pos.get().debug_str() ) ;
                ret  = ret & parseUsing(pos,table_join,expected);

                LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"2  using pos is :" + pos.get().debug_str() ) ;

                if(table_join->left_table_expression){
                    table_join->children.emplace_back(table_join->left_table_expression);
                }

                if(table_join->right_table_expression){
                    table_join->children.emplace_back(table_join->right_table_expression);
                }


                res->table_join = table_join;


            } else {   // subQuery
                ret =  ParserTableExpression().parse(pos,  res->table_expression, expected);
            }
        }
    } else if(ParserArrayJoin().parse(pos, res->array_join, expected)){


    } else {

        LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery")," not join or subquery or arrayJoin  " + pos_info);

        ret = false;
    }

    if (res->table_expression)
        res->children.emplace_back(res->table_expression);
    if (res->table_join)
        res->children.emplace_back(res->table_join);
    if (res->array_join)
        res->children.emplace_back(res->array_join);



    node = res;

    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"ASTTablesInSelectQueryElement  query is :" + queryToString(node) ) ;

    std::stringstream dbg_str;
    node->dumpTree(dbg_str);
    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery"),"ASTTablesInSelectQueryElement  node tree is :" + dbg_str.str()) ;
    return ret;
}

bool ParserTablesInSelectQueryElement::parseUsing(Pos & pos, std::shared_ptr<ASTTableEnhanceJoin> & table_join,Expected & expected){

    if (ParserKeyword("USING").ignore(pos, expected))
    {
        /// Expression for USING could be in parentheses or not.
        bool in_parens = pos->type == TokenType::OpeningRoundBracket;
        if (in_parens)
            ++pos;

        if (!ParserExpressionList(false).parse(pos, table_join->using_expression_list, expected))
            return false;

        if (in_parens)
        {
            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
    }
    else if (ParserKeyword("ON").ignore(pos, expected))
    {
        /// OR is operator with lowest priority, so start parsing from it.
        if (!ParserLogicalOrExpression().parse(pos, table_join->on_expression, expected))
            return false;
    }
    else
    {
        return false;
    }

    if (table_join->using_expression_list)
        table_join->children.emplace_back(table_join->using_expression_list);
    if (table_join->on_expression)
        table_join->children.emplace_back(table_join->on_expression);


    return true;

}



bool ParserTablesInSelectQueryElement::parseJoinInfo(Pos & pos, Expected & expected,std::shared_ptr<ASTTableEnhanceJoin> & table_join){

    if (ParserKeyword("GLOBAL").ignore(pos))
        table_join->locality = ASTTableEnhanceJoin::Locality::Global;
    else if (ParserKeyword("LOCAL").ignore(pos))
        table_join->locality = ASTTableEnhanceJoin::Locality::Local;

    if (ParserKeyword("ANY").ignore(pos))
        table_join->strictness = ASTTableEnhanceJoin::Strictness::Any;
    else if (ParserKeyword("ALL").ignore(pos))
        table_join->strictness = ASTTableEnhanceJoin::Strictness::All;

    if (ParserKeyword("INNER").ignore(pos))
        table_join->kind = ASTTableEnhanceJoin::Kind::Inner;
    else if (ParserKeyword("LEFT").ignore(pos))
        table_join->kind = ASTTableEnhanceJoin::Kind::Left;
    else if (ParserKeyword("RIGHT").ignore(pos))
        table_join->kind = ASTTableEnhanceJoin::Kind::Right;
    else if (ParserKeyword("FULL").ignore(pos))
        table_join->kind = ASTTableEnhanceJoin::Kind::Full;
    else if (ParserKeyword("CROSS").ignore(pos))
        table_join->kind = ASTTableEnhanceJoin::Kind::Cross;
    else
    {
        /// Maybe need use INNER by default as in another DBMS.
        return false;
    }

    if (table_join->strictness != ASTTableEnhanceJoin::Strictness::Unspecified
        && table_join->kind == ASTTableEnhanceJoin::Kind::Cross)
        throw Exception("You must not specify ANY or ALL for CROSS JOIN.", ErrorCodes::SYNTAX_ERROR);

    /// Optional OUTER keyword for outer joins.
    if (table_join->kind == ASTTableEnhanceJoin::Kind::Left
        || table_join->kind == ASTTableEnhanceJoin::Kind::Right
        || table_join->kind == ASTTableEnhanceJoin::Kind::Full)
    {
        ParserKeyword("OUTER").ignore(pos);
    }

    if (!ParserKeyword("JOIN").ignore(pos, expected))
        return false;

    return true;
}

bool ParserTablesInSelectQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{

    LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery")," #### parseImpl ") ;
    return  parseEnhanceJoinImpl(pos,node,expected);

    auto res = std::make_shared<ASTTablesInSelectQueryElement>();

    if (is_first)
    {
        if (!ParserTableExpression().parse(pos, res->table_expression, expected))
            return false;
    }
    else if (ParserArrayJoin().parse(pos, res->array_join, expected))
    {
    }
    else
    {
        auto table_join = std::make_shared<ASTTableJoin>();

        if (pos->type == TokenType::Comma)
        {
            ++pos;
            table_join->kind = ASTTableJoin::Kind::Comma;
        }
        else
        {
            if (ParserKeyword("GLOBAL").ignore(pos))
                table_join->locality = ASTTableJoin::Locality::Global;
            else if (ParserKeyword("LOCAL").ignore(pos))
                table_join->locality = ASTTableJoin::Locality::Local;

            if (ParserKeyword("ANY").ignore(pos))
                table_join->strictness = ASTTableJoin::Strictness::Any;
            else if (ParserKeyword("ALL").ignore(pos))
                table_join->strictness = ASTTableJoin::Strictness::All;

            if (ParserKeyword("INNER").ignore(pos))
                table_join->kind = ASTTableJoin::Kind::Inner;
            else if (ParserKeyword("LEFT").ignore(pos))
                table_join->kind = ASTTableJoin::Kind::Left;
            else if (ParserKeyword("RIGHT").ignore(pos))
                table_join->kind = ASTTableJoin::Kind::Right;
            else if (ParserKeyword("FULL").ignore(pos))
                table_join->kind = ASTTableJoin::Kind::Full;
            else if (ParserKeyword("CROSS").ignore(pos))
                table_join->kind = ASTTableJoin::Kind::Cross;
            else
            {
                /// Maybe need use INNER by default as in another DBMS.
                return false;
            }

            if (table_join->strictness != ASTTableJoin::Strictness::Unspecified
                && table_join->kind == ASTTableJoin::Kind::Cross)
                throw Exception("You must not specify ANY or ALL for CROSS JOIN.", ErrorCodes::SYNTAX_ERROR);

            /// Optional OUTER keyword for outer joins.
            if (table_join->kind == ASTTableJoin::Kind::Left
                || table_join->kind == ASTTableJoin::Kind::Right
                || table_join->kind == ASTTableJoin::Kind::Full)
            {
                ParserKeyword("OUTER").ignore(pos);
            }

            if (!ParserKeyword("JOIN").ignore(pos, expected))
                return false;
        }

        if (!ParserTableExpression().parse(pos, res->table_expression, expected))
            return false;

        if (table_join->kind != ASTTableJoin::Kind::Comma
            && table_join->kind != ASTTableJoin::Kind::Cross)
        {
            if (ParserKeyword("USING").ignore(pos, expected))
            {
                /// Expression for USING could be in parentheses or not.
                bool in_parens = pos->type == TokenType::OpeningRoundBracket;
                if (in_parens)
                    ++pos;

                if (!ParserExpressionList(false).parse(pos, table_join->using_expression_list, expected))
                    return false;

                if (in_parens)
                {
                    if (pos->type != TokenType::ClosingRoundBracket)
                        return false;
                    ++pos;
                }
            }
            else if (ParserKeyword("ON").ignore(pos, expected))
            {
                /// OR is operator with lowest priority, so start parsing from it.
                if (!ParserLogicalOrExpression().parse(pos, table_join->on_expression, expected))
                    return false;
            }
            else
            {
                return false;
            }
        }

        if (table_join->using_expression_list)
            table_join->children.emplace_back(table_join->using_expression_list);
        if (table_join->on_expression)
            table_join->children.emplace_back(table_join->on_expression);

        res->table_join = table_join;
    }
    //jungle comment: table_join and table_expression is in the same level

    if (res->table_expression)
        res->children.emplace_back(res->table_expression);
    if (res->table_join)
        res->children.emplace_back(res->table_join);
    if (res->array_join)
        res->children.emplace_back(res->array_join);

    node = res;
    return true;
}


    bool ParserTablesInSelectQueryElement::parseEnhanceJoinImpl(Pos & pos, ASTPtr & node, Expected & expected)
    {

        LOG_DEBUG(&Logger::get("ParserTablesInSelectQuery")," #### parseEnhanceJoinImpl ") ;
        //return  parseEnhanceJoinImpl(pos,node,expected);

        auto res = std::make_shared<ASTTablesInSelectQueryElement>();

        if (is_first)
        {
            if (!ParserTableExpression().parse(pos, res->table_expression, expected))
                return false;
        }
        else if (ParserArrayJoin().parse(pos, res->array_join, expected))
        {
        }
        else
        {
            auto table_join = std::make_shared<ASTTableEnhanceJoin>();

            if (pos->type == TokenType::Comma)
            {
                ++pos;
                table_join->kind = ASTTableEnhanceJoin::Kind::Comma;
            }
            else
            {
                if (ParserKeyword("GLOBAL").ignore(pos))
                    table_join->locality = ASTTableEnhanceJoin::Locality::Global;
                else if (ParserKeyword("LOCAL").ignore(pos))
                    table_join->locality = ASTTableEnhanceJoin::Locality::Local;

                if (ParserKeyword("ANY").ignore(pos))
                    table_join->strictness = ASTTableEnhanceJoin::Strictness::Any;
                else if (ParserKeyword("ALL").ignore(pos))
                    table_join->strictness = ASTTableEnhanceJoin::Strictness::All;

                if (ParserKeyword("INNER").ignore(pos))
                    table_join->kind = ASTTableEnhanceJoin::Kind::Inner;
                else if (ParserKeyword("LEFT").ignore(pos))
                    table_join->kind = ASTTableEnhanceJoin::Kind::Left;
                else if (ParserKeyword("RIGHT").ignore(pos))
                    table_join->kind = ASTTableEnhanceJoin::Kind::Right;
                else if (ParserKeyword("FULL").ignore(pos))
                    table_join->kind = ASTTableEnhanceJoin::Kind::Full;
                else if (ParserKeyword("CROSS").ignore(pos))
                    table_join->kind = ASTTableEnhanceJoin::Kind::Cross;
                else
                {
                    /// Maybe need use INNER by default as in another DBMS.
                    return false;
                }

                if (table_join->strictness != ASTTableEnhanceJoin::Strictness::Unspecified
                    && table_join->kind == ASTTableEnhanceJoin::Kind::Cross)
                    throw Exception("You must not specify ANY or ALL for CROSS JOIN.", ErrorCodes::SYNTAX_ERROR);

                /// Optional OUTER keyword for outer joins.
                if (table_join->kind == ASTTableEnhanceJoin::Kind::Left
                    || table_join->kind == ASTTableEnhanceJoin::Kind::Right
                    || table_join->kind == ASTTableEnhanceJoin::Kind::Full)
                {
                    ParserKeyword("OUTER").ignore(pos);
                }

                if (!ParserKeyword("JOIN").ignore(pos, expected))
                    return false;
            }

            ASTPtr right_table_expression  ;
            //if (!ParserTableExpression().parse(pos, res->table_expression, expected))
            //    return false;
            if (!ParserTableExpression().parse(pos, right_table_expression, expected))
                return false;
            table_join->right_table_expression = right_table_expression;

            if (table_join->kind != ASTTableEnhanceJoin::Kind::Comma
                && table_join->kind != ASTTableEnhanceJoin::Kind::Cross)
            {
                if (ParserKeyword("USING").ignore(pos, expected))
                {
                    /// Expression for USING could be in parentheses or not.
                    bool in_parens = pos->type == TokenType::OpeningRoundBracket;
                    if (in_parens)
                        ++pos;

                    if (!ParserExpressionList(false).parse(pos, table_join->using_expression_list, expected))
                        return false;

                    if (in_parens)
                    {
                        if (pos->type != TokenType::ClosingRoundBracket)
                            return false;
                        ++pos;
                    }
                }
                else if (ParserKeyword("ON").ignore(pos, expected))
                {
                    /// OR is operator with lowest priority, so start parsing from it.
                    if (!ParserLogicalOrExpression().parse(pos, table_join->on_expression, expected))
                        return false;
                }
                else
                {
                    return false;
                }
            }

            if (table_join->using_expression_list)
                table_join->children.emplace_back(table_join->using_expression_list);
            if (table_join->on_expression)
                table_join->children.emplace_back(table_join->on_expression);

            if (table_join->right_table_expression)
                table_join->children.emplace_back(table_join->right_table_expression);
            res->table_join = table_join;
        }
        //jungle comment: table_join and table_expression is in the same level

        if (res->table_expression)
            res->children.emplace_back(res->table_expression);
        if (res->table_join)
            res->children.emplace_back(res->table_join);
        if (res->array_join)
            res->children.emplace_back(res->array_join);

        node = res;
        return true;
    }



bool ParserTablesInSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)  //jungle coment: parse From clause
{
    /*
    auto res = std::make_shared<ASTTablesInSelectQuery>();

    ASTPtr child;

    if (ParserTablesInSelectQueryElement(true).parse(pos, child, expected))
        res->children.emplace_back(child);
    else
        return false;

    while (ParserTablesInSelectQueryElement(false).parse(pos, child, expected))
        res->children.emplace_back(child);

    node = res;
    return true;
    */

    auto res = std::make_shared<ASTTablesInSelectQuery>();

    ASTPtr child1;
    ASTPtr child2;

    if (ParserTablesInSelectQueryElement(true).parse(pos, child1, expected))
        res->children.emplace_back(child1);
    else
        return false;

    while (ParserTablesInSelectQueryElement(false).parse(pos, child2, expected)){

        ASTTableEnhanceJoin * join  =  typeid_cast<ASTTableEnhanceJoin * >( typeid_cast< ASTTablesInSelectQueryElement*>(child2.get())->table_join.get());
        if( join ){

            ASTPtr pre_table =  typeid_cast< ASTTablesInSelectQueryElement*>(res->children.back().get())->table_expression;
            res->children.clear();
            join->left_table_expression  = pre_table ;
            join->children.emplace_back(join->left_table_expression );

            res->children.emplace_back(child2);
        }else{
            res->children.emplace_back(child2);
            // array join not change
        }

    }


    node = res;
    return true;
}

}
