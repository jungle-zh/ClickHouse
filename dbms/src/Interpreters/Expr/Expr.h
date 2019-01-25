//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_EXPR_H
#define CLICKHOUSE_EXPR_H


#include <vector>
#include <string>

namespace DB {

class ExprActions;
class TExpr ;


class Expr {

using ExprPtr = std::shared_ptr<Expr>;
using ExprChildsPtr = std::shared_ptr<std::vector<ExprPtr>>;
public:
    void fromThrift(TExpr & expr);
    void getExprActions(ExprActions & actions )  ;
    virtual std::vector<int>  needColumnId() {
        return  std::vector<int>();
    }
    //void getType();
    virtual std::string getName() const ;

    std::vector<ExprPtr> & getChilds() { return  childs ;}


protected:
    std::vector<ExprPtr> childs;
    int id;
    std::string name ;


};


}



#endif //CLICKHOUSE_EXPR_H
