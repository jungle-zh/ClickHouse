//
// Created by jungle on 19-6-16.
//

#pragma once

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>
#include "ExecNode.h"

namespace DB {


class ProjectExecNode  : public  ExecNode{

public:

    void serialize(WriteBuffer & buffer);
    static  std::shared_ptr<ProjectExecNode> deseralize(ReadBuffer & buffer);

    Block read() override;

    //NamesWithAliases projection;
    std::shared_ptr<ExpressionActions> actions ;
};



}