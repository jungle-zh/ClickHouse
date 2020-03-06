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


    ProjectExecNode(std::shared_ptr<ExpressionActions> actions_){
        actions = actions_;
    }

    void serialize(WriteBuffer & buffer);
    static  std::shared_ptr<ExecNode> deseralize(ReadBuffer & buffer,Context * context);

     void  readPrefix(std::shared_ptr<DataExechangeClient>) override {};
     void  readSuffix() override {};
    Block read() override;
    Block getHeader(bool isAnalyze) override;
    Block getInputHeader() override;

    //NamesWithAliases projection;
    std::shared_ptr<ExpressionActions> actions ;
    Block inputHeader ;
    std::string getName() override { return  "projectExecNode";}
    //Block header ;
};



}