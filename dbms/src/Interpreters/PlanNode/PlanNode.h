//
// Created by Administrator on 2019/3/31.
//

#ifndef CLICKHOUSE_PLANNODE_H
#define CLICKHOUSE_PLANNODE_H


#include <vector>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>



namespace DB {


class  PlanNode {


public:
    using PlanNodePtr = std::shared_ptr<PlanNode>;
public:


    //virtual void serialize(WriteBuffer & ostr) ;
    //virtual void deserialze(ReadBuffer & istr) ;

    void addChild(PlanNodePtr child);
    Block getHeader()  { return isHeaderInited ? header :initHeader() ; }


    virtual  Block  initHeader() ;


    PlanNodePtr getFirstChild() { return  childs[0]; }

private:

    //std::shared_ptr<PlanNodePtr> father;
    std::vector<PlanNodePtr> childs;
    Block  header;
    bool   isHeaderInited ;
    ExpressionActionsPtr     expressionActions;

};


}

#endif //CLICKHOUSE_PLANNODE_H
