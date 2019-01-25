//
// Created by admin on 19/1/18.
//

#include <Interpreters/Expr/FunctionExpr.h>

namespace DB {

void  FunctionExpr::getExprActions(ExprAcitons & actions){

    for(size_t i=0 ;i<child.size() ; ++i){

        if(static_cast<Colu>(*child[i].get()))
    }



}

std::vector<int> FunctionExpr::needColumnId(){

}

}


