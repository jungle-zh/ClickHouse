//
// Created by jungle on 19-6-29.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {



class UnionExecNode  : public ExecNode {

    //void serialize(WriteBuffer & buffer);
    //static  std::shared_ptr<ExecNode> deseralize(ReadBuffer & buffer);
public:
    Block read() override;
    Block getHeader(bool isAnalyze) override;
    Block getInputHeader() override ;

    void serialize(WriteBuffer & buffer){ (void)buffer;}
    static  std::shared_ptr<ExecNode> deseralize(ReadBuffer & buffer);
    std::string getName() override { return  "unionExecNode";}

};



}