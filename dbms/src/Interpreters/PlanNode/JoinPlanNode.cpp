//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/JoinPlanNode.h>
#include <Interpreters/PlanNode/ExechangeNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>

namespace DB {


    std::shared_ptr<ExecNode> JoinPlanNode::createExecNode() {

        assert(hashTable == "right" || hashTable ==  "left");
        assert(header);
        return  std::make_shared<JoinExecNode>(
                    joinKeys,
                    hashTable == "right" ? inputLeftHeader : inputRightHeader,
                    hashTable == "right" ? inputRightHeader : inputLeftHeader,
                    joinKind,
                    strictness,
                    header
                );
    }
    Block JoinPlanNode::getHeader()  {
        if(header)
            return  header;
        Block joinHeader ;
        for(ColumnWithTypeAndName e : inputLeftHeader.getColumnsWithTypeAndName()){
            joinHeader.insert(e);
        }
        std::set<std::string > keys;
        for(std::string key : joinKeys){
            keys.insert(key);
        }
        for(ColumnWithTypeAndName e : inputRightHeader.getColumnsWithTypeAndName()){
            if(!keys.count(e.name)){
                joinHeader.insert(e);
            }

        }
        header = joinHeader;
        return  joinHeader;
    }

    void JoinPlanNode::setHashTable(std::string table) {
        hashTable = table;
    }
    void JoinPlanNode::setHashTableStageId(std::string hashTableStage_) {
        hashTableStage = hashTableStage_;
    }
}
