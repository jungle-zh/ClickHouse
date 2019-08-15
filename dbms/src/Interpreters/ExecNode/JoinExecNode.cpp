//
// Created by usser on 2019/6/15.
//

#include  <Interpreters/ExecNode/JoinExecNode.h>

namespace DB {

    Block JoinExecNode::getHeader(bool isAnalyze)  {
        (void) isAnalyze;
        Block joinHeader ;
        for(ColumnWithTypeAndName e : mainTableHeader.getColumnsWithTypeAndName()){
            joinHeader.insert(e);
        }
        std::set<std::string > keys;
        for(std::string key : joinKey){
            keys.insert(key);
        }
        for(ColumnWithTypeAndName e : hashTableHeader.getColumnsWithTypeAndName()){
            if(!keys.count(e.name)){
                joinHeader.insert(e);
            }

        }
        return  joinHeader;
    }
    /*
    void JoinExecNode::readPrefix(){


    }
     */

    Block JoinExecNode::read() {

        Block res = children->read();
        if (!res)
            return res;
        join->joinBlock(res);
        return res;

    }

    void JoinExecNode::serialize(DB::WriteBuffer &buffer) {

        writeVarUInt(joinKey.size(), buffer);
        for(size_t i=0 ;i < joinKey.size(); ++i){
            writeStringBinary(joinKey[i],buffer);
        }
        ExecNode::serializeHeader(mainTableHeader,buffer);
        ExecNode::serializeHeader(hashTableHeader,buffer);
        writeStringBinary(joinKind, buffer);
        writeStringBinary(strictness, buffer);
    }
    std::shared_ptr<ExecNode> JoinExecNode::deserialize(DB::ReadBuffer &buffer) {
        size_t joinKeySize ;
        readVarUInt(joinKeySize,buffer);
        Names keys;
        for(size_t i=0 ; i< joinKeySize;++i){
            std::string key;
            readStringBinary(key,buffer);
            keys.push_back(key);
        }

        Block inputLeftHeader = ExecNode::deSerializeHeader(buffer);
        Block inputRightHeader = ExecNode::deSerializeHeader(buffer);
        std::string joinKind ;
        std::string strictness ;
        readStringBinary(joinKind,buffer);
        readStringBinary(strictness, buffer);
        return  std::make_shared<JoinExecNode>(keys,inputLeftHeader,inputRightHeader,joinKind,strictness);

    }

}
