//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_LITERALEXPR_H
#define CLICKHOUSE_LITERALEXPR_H

#include <Interpreters/Expr/Expr.h>
#include <DataTypes/IDataType.h>
#include <Core/Field.h>

namespace DB {
class LiteralExpr : public Expr  {


public:

    Field value;

    std::vector<int>  needColumnId()  override ;

    static  Field converToField(std::string & data, IDataType type);




};

}


#endif //CLICKHOUSE_LITERALEXPR_H
