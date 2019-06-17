//
// Created by Administrator on 2019/5/4.
//

#include <Interpreters/PlanNode/ScanPlanNode.h>

namespace DB {

Block ScanPlanNode::read(){


//MergeTreeBaseBlockInputStream read()
return table->read();


}



}
