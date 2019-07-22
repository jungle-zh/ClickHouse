//
// Created by usser on 2019/6/15.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Interpreters/Aggregator.h>


namespace DB {


class MergeExecNode  : public ExecNode {


public:
    MergeExecNode(Block & inputHeader_,NamesAndTypesList &  aggregationKeys_ ,
                  NamesAndTypesList & aggregatedColumns_, AggregateDescriptions  & aggregateDescriptions_ ,Context & context_):
                  inputHeader(inputHeader_),
                  aggregationKeys(aggregationKeys_),
                  aggregateColumns(aggregatedColumns_),
                  aggregateDescriptions(aggregateDescriptions_),
                  context(context_){

    }

    void  readPrefix() override;
    void  readSuffix() override;
    Block read() override ;
    Block getHeader ()  override;

    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;
    void  serializeAggDesc(WriteBuffer & buffer);
    static AggregateDescriptions deserializeAggDesc (ReadBuffer & buffer);

private:



    Block inputHeader ;
    NamesAndTypesList aggregationKeys;
    NamesAndTypesList aggregateColumns;
    AggregateDescriptions  aggregateDescriptions ;

    std::unique_ptr<Aggregator> aggregator;
    std::unique_ptr<Aggregator::Params> params;


    Settings  settings  ;

    bool allow_to_use_two_level_group_by;
    bool overflow_row;

    Context & context ;

    bool  final = true;
    size_t max_threads;

    bool executed = false;
    BlocksList blocks;
    BlocksList::iterator it;
};

}



