//
// Created by jungle on 19-6-16.
//

#include "ProjectExecNode.h"
namespace  DB {


    void ProjectExecNode::serialize(DB::WriteBuffer &buffer) {
        ExecNode::serializeExpressActions(*actions,buffer);
    }

    std::shared_ptr<ExecNode> ProjectExecNode::deseralize(DB::ReadBuffer &buffer) {

        std::shared_ptr<ExpressionActions> actions = ExecNode::deSerializeExpressActions(buffer);
        return  std::make_shared<ProjectExecNode>(actions);

    }

    Block ProjectExecNode::read() {

        Block block = children->read();
        actions->execute(block);
        return block;
    }
    Block ProjectExecNode::getHeader() {
        Block header  = inputHeader;
        actions->execute(header);
        return  header ;

    }
    Block ProjectExecNode::getInputHeader() {
        return  inputHeader;
    }



}