//
// Created by jungle on 19-6-29.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {



class UnionExecNode  : public ExecNode {

    //void serialize(WriteBuffer & buffer);
    //static  std::shared_ptr<ExecNode> deseralize(ReadBuffer & buffer);
    Block read() override;
    Block getHeader() override;
    Block getInputHeader() override ;
};



}