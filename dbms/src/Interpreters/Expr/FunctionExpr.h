//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_FUNCTIONEXPR_H
#define CLICKHOUSE_FUNCTIONEXPR_H

#include <Interpreters/Expr/Expr.h>
#include <Interpreters/Expr/ExprAction.h>
#include <Interpreters/Expr/LiteralExpr.h>

namespace DB {

using FunctionExprParam =  std::shared_ptr<std::vector<LiteralExpr>>;
class ExprAcitons;
class FunctionExpr : public Expr {

public:
    void  getExprActions(ExprAcitons & actions) override ;

    std::vector<int>  needColumnId()  override ;
    FunctionExprParam  getParams(){ return  params;}


private:
    FunctionExprParam    params;



};



}




#endif //CLICKHOUSE_FUNCTIONEXPR_H
