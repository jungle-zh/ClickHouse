#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** List of single or multiple JOIN-ed tables or subqueries in SELECT query, with ARRAY JOINs and SAMPLE, FINAL modifiers.
  */

class  ASTTableEnhanceJoin;
class ParserTablesInSelectQuery : public IParserBase
{
protected:
    const char * getName() const { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserTablesInSelectQueryElement : public IParserBase
{
public:
    ParserTablesInSelectQueryElement(bool is_first) : is_first(is_first) {}

    enum elementType {
        JOIN,
        SUBQUERY,
        TABLE,
        ARRAYJOIN
    };

protected:
    const char * getName() const { return "table, table function, subquery or list of joined tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
    bool parseEnhanceJoinImpl(Pos & pos, ASTPtr & node,Expected & expected);
    bool parseUsing(Pos & pos,  std::shared_ptr<ASTTableEnhanceJoin> & node,Expected & expected);
    bool parseJoinInfo(Pos & pos, Expected & expected,std::shared_ptr<ASTTableEnhanceJoin> & table_join);
    elementType getElementType(Pos & pos);


private:
    bool is_first;
};


class ParserTableExpression : public IParserBase
{
protected:
    const char * getName() const { return "table or subquery or table function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


class ParserArrayJoin : public IParserBase
{
protected:
    const char * getName() const { return "array join"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


}
