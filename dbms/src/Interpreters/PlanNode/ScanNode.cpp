//
// Created by Administrator on 2019/5/4.
//

#include <Interpreters/PlanNode/ScanNode.h>

namespace DB {

Block ScanNode::read(){


//MergeTreeBaseBlockInputStream read()
return table->read();


}



}
