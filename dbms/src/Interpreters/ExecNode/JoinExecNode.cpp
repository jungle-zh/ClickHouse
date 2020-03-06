//
// Created by usser on 2019/6/15.
//

#include  <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/DataExechangeClient.h>

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


    void JoinExecNode::readPrefix(std::shared_ptr<DataExechangeClient> client) {

        LOG_DEBUG(log,"join start read hashTable :" + hashTableStageId);
        while(Block hashBlock = client->read(hashTableStageId)){
            getJoin()->insertFromBlock(hashBlock);
        }

        LOG_DEBUG(log,"join read all hashTable :" << hashTableStageId  << " block size : " << getJoin()->blocks.size(););

    }

    Block JoinExecNode::read() {

        Block res = children->read();
        if (!res)
            return res;
        //todo verify
        if(!ExecNode::verify(res,mainTableHeader)){
            throw Exception("join exec child block don't match ");
        }

        join->joinBlock(res);

        LOG_DEBUG(log,"hash table block size: " << join->blocks.size() << ",joined block size:" << res.rows());
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
        writeStringBinary(hashTableStageId,buffer);
        ExecNode::serializeHeader(adjustHeader,buffer);
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
        std::string hashTableStageId;
        readStringBinary(joinKind,buffer);
        readStringBinary(strictness, buffer);
        readStringBinary(hashTableStageId,buffer);
        Block adjustHeader = ExecNode::deSerializeHeader(buffer);
        return  std::make_shared<JoinExecNode>(keys,inputLeftHeader,inputRightHeader,joinKind,strictness,adjustHeader,hashTableStageId);

    }

}
