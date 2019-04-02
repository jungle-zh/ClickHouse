//
// Created by Administrator on 2019/4/3.
//

#include <Interpreters/PlanNode/ProjectNode.h>

namespace DB {

std::shared_ptr<Block> ProjectNode::initHeader() {

    Block childHeader =  getFirstChild()->getHeader();

    expressionActions->execute(header);


}

}