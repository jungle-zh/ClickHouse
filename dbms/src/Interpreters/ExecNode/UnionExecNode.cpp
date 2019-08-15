//
// Created by jungle on 19-6-29.
//

#include "UnionExecNode.h"

namespace DB {

    Block UnionExecNode::read() {
        return  children->read(); // children is taskReceiverExecNode
    }

    Block UnionExecNode::getHeader(bool isAnalyze) {
        (void)isAnalyze;
        return  header;
    }
    Block UnionExecNode::getInputHeader() {
        return  header;
    }
    void UnionExecNode::serialize(DB::WriteBuffer &buffer) {
        ExecNode::serializeHeader(header,buffer);
    }
    std::shared_ptr<ExecNode> UnionExecNode::deseralize(DB::ReadBuffer &buffer) {
        Block header =  ExecNode::deSerializeHeader(buffer);
        return std::make_shared<UnionExecNode>(header);
    }


}