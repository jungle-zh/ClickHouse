//
// Created by jungle on 19-6-16.
//

#include "ProjectExecNode.h"
namespace  DB {


    void ProjectExecNode::serialize(DB::WriteBuffer &buffer) {
        ExecNode::serializeExpressActions(*actions,buffer);
    }

    std::shared_ptr<ExecNode> ProjectExecNode::deseralize(DB::ReadBuffer &buffer ,Context  * context) {

        std::shared_ptr<ExpressionActions> actions = ExecNode::deSerializeExpressActions(buffer,context);
        return  std::make_shared<ProjectExecNode>(actions);

    }

    Block ProjectExecNode::read() {

        Block block = children->read();
        actions->execute(block);
        return block;
    }
    Block ProjectExecNode::getHeader(bool isAnalyze) {
        (void) isAnalyze;
        Block header  = inputHeader;
        actions->execute(header);
        return  header ;

    }
    Block ProjectExecNode::getInputHeader() {
        return  inputHeader;
    }



}