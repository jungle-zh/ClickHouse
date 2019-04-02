//
// Created by Administrator on 2019/3/31.
//

#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class  ProjectNode  : public  PlanNode {

public:
    Block initHeader() override;




};


}

