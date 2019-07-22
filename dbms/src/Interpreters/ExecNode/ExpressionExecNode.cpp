//
// Created by jungle on 19-7-21.
//

#include "ExpressionExecNode.h"

namespace DB {


    Block ExpressionExecNode::read() {
        Block res = children->read();
        if (!res)
            return res;
        expression->execute(res);
        return res;
    }

    Block ExpressionExecNode::getHeader()
    {
        Block res = children->getHeader();
        expression->execute(res);
        return res;
    }

    Block ExpressionExecNode::getInputHeader() {
        Block res = children->getHeader();
        return  res;
    }
    void ExpressionExecNode::serialize(DB::WriteBuffer &buffer) {
        ExecNode::serializeExpressActions(*expression,buffer);
    }

    std::shared_ptr<ExecNode> ExpressionExecNode::deserialize(DB::ReadBuffer &buffer) {

        return std::make_shared<ExpressionExecNode>(ExecNode::deSerializeExpressActions(buffer));

    }


}