//
// Created by usser on 2019/6/15.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/Join.h>

namespace DB {



class JoinExecNode : public ExecNode {


public:

    void  readPrefix() override;
    void  readSuffix() override;
    Block readImpl() override ;
    Block getHeader () const override;


    JoinExecNode(Names  & joinKey_ , Block & inputLeftHeader_ , Block & inputRightHeader_,
                 ASTTableJoin::Kind kind_,ASTTableJoin::Strictness strictness_):
                 joinKey(joinKey_),
                 inputLeftHeader(inputLeftHeader_),
                 inputRightHeader(inputRightHeader_),
                 kind(kind_),
                 strictness(strictness_){

    }


private:
    Names joinKey;
    Block inputLeftHeader;
    Block inputRightHeader;
    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness  strictness;

    std::unique_ptr<Join>  join;

    Settings settings ;



};



}