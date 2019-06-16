//
// Created by usser on 2019/6/15.
//

#include  <Interpreters/ExecNode/JoinExecNode.h>

namespace DB {

    Block JoinExecNode::getHeader() const {

        Block joinHeader ;
        for(ColumnWithTypeAndName e : inputLeftHeader.getColumnsWithTypeAndName()){
            joinHeader.insert(e);
        }
        std::set<std::string > keys;
        for(std::string key : joinKey){
            keys.insert(key);
        }
        for(ColumnWithTypeAndName e : inputRightHeader.getColumnsWithTypeAndName()){
            if(!keys.count(e.name)){
                joinHeader.insert(e);
            }

        }
        return  joinHeader;
    }
    void JoinExecNode::readPrefix(){


         join = std::make_unique<Join>(
                inputLeftHeader.getNamesAndTypesList().getNames(), inputRightHeader.getNamesAndTypesList().getNames(),
                settings.join_use_nulls, SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
                kind, strictness);

         join->setSampleBlock(inputRightHeader);

         assert(children.size() == 2);
         while(Block block = children[1]->read()){
             join->insertFromBlock(block);
         }

    }

    Block JoinExecNode::readImpl() {

        Block res = children[0]->read();
        if (!res)
            return res;
        join->joinBlock(res);
        return res;

    }


}
