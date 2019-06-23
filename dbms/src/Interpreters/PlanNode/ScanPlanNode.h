//
// Created by Administrator on 2019/3/31.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class ScanPlanNode : public PlanNode {

    ScanPlanNode(std::string dbName_ ,std::string tableName_){
        dbName = dbName_;
        tableName = tableName_;
    }

private:

    std::string dbName ;
    std::string tableName;


};

}

