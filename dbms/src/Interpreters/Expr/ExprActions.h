//
// Created by admin on 19/1/18.
//

#ifndef CLICKHOUSE_EXPRACTIONS_H
#define CLICKHOUSE_EXPRACTIONS_H

#include <Core/Block.h>


namespace DB {

class ExprAction ;

class ExprActions {

 using ExprActionsPtr = std::shared_ptr<ExprAction> ;

public:
    ExprActions(Block & input) { sample_block = input ; }

    Block  getSampleBlock(){ return  sample_block; }
    std::vector<ExprActionsPtr> getActions(){ return actions; }
    void initWithHeader(Block  block) { sample_block = block ;}

    void execute(Block & block);

    void dumpActions ();
    bool isInited;


    Block sample_block;
    std::vector<ExprActionsPtr>  actions;

};



}




#endif //CLICKHOUSE_EXPRACTIONS_H
