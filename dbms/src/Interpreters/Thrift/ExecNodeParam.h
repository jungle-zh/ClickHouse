//
// Created by admin on 19/1/20.
//

#ifndef CLICKHOUSE_TEXECNODE_H
#define CLICKHOUSE_TEXECNODE_H

#include <vector>
#include <Interpreters/Expr/Expr.h>
#include <Thrift/BackendService.h>
namespace DB {





class ExecNodeParam {

public:
    palo::BackendServiceIf  in ;
    std::vector<Expr> aggExprs;
    std::vector<Expr> groupExprs;

};


}



#endif //CLICKHOUSE_TEXECNODE_H
