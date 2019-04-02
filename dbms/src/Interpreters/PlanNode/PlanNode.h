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

using PlanNodePtr = std::shared_ptr<PlanNode>;

public:


    //virtual void serialize(WriteBuffer & ostr) ;
    //virtual void deserialze(ReadBuffer & istr) ;

    void addChild(PlanNodePtr child);


private:

    std::vector<PlanNodePtr> childs;



};


}

#endif //CLICKHOUSE_PLANNODE_H
