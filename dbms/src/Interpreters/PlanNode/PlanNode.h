//
// Created by Administrator on 2019/3/31.
//
#pragma once

#include <vector>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>


namespace DB {


class  PlanNode {


public:
    using PlanNodePtr = std::shared_ptr<PlanNode>;
public:

    PlanNode();

    //virtual void serialize(WriteBuffer & ostr) ;
    //virtual void deserialze(ReadBuffer & istr) ;

   // void addChild(PlanNodePtr child);
    void clearChild();

    void setUnaryChild();
    void setLeftChild();
    void setRightChild();

    std::string virtual type();
    virtual  Block getHeader();
    std::string getName() ;


    virtual Block  read();
    virtual void init();

    static void  serialize (WriteBuffer & buffer);

    static void  deserialize (WriteBuffer & buffer,PlanNode * res);


    PlanNodePtr getUnaryChild() { return  childs[0]; }

    PlanNodePtr getLeftChild() { return childs[0]; }

    PlanNodePtr getRightChild() { return  childs[1];}

private:

    //std::shared_ptr<PlanNodePtr> father;
    std::vector<PlanNodePtr> childs;
    Block  header;
    bool   isHeaderInited ;
    //ExpressionActionsPtr     expressionActions;

};


}
