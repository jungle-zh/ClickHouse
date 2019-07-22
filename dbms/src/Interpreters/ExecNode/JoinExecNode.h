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
    Block read() override ;
    Block getHeader () override;

    virtual ~JoinExecNode() {}
    JoinExecNode(Names  & joinKey_ , Block & inputLeftHeader_ , Block & inputRightHeader_,
                 std::string joinKind_,std::string strictness_):
                 joinKey(joinKey_),
                 inputLeftHeader(inputLeftHeader_),
                 inputRightHeader(inputRightHeader_),
                 joinKind(joinKind_),
                 strictness(strictness_){

    }

    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;


public:

    Join * getJoin(){ return  join.get();}
private:
    Names joinKey;
    Block inputLeftHeader;
    Block inputRightHeader;
    std::string joinKind;
    std::string  strictness;

    std::unique_ptr<Join>  join;

    Settings settings ;



};



}