//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_PLANNODE_H
#define CLICKHOUSE_PLANNODE_H


#include <memory>
#include <vector>
#include <Core/Block.h>
#include <Interpreters/Thrift/ExecNodeParam.h>
#include <Interpreters/Context.h>
#include <Interpreters/Expr/ExprActions.h>

namespace  DB {


class ExecNode {


using ExecNodePtr = std::shared_ptr<ExecNode>;
using ExprActionsPtr = std::shared_ptr<ExprActions>;
public:
    virtual void prepare();

    virtual Block getNext();

    virtual void finish();

    virtual Block getHeader();

    static BlockInputStreamPtr  buildChildStream(ExecNodePtr & node ) ;

    static std::vector<BlockInputStreamPtr>  buildChildStreams(std::vector<ExecNodePtr > &  nodes);

protected:
    std::vector<ExecNodePtr> childs;
    ExecNodeParam param;
    Context context;

    Block inputSampleBlock ;
    ExprActionsPtr actions;


};

}


#endif //CLICKHOUSE_PLANNODE_H
