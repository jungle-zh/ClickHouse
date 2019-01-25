//
// Created by admin on 19/1/20.
//

#ifndef CLICKHOUSE_AGGREGATIONNODE_H
#define CLICKHOUSE_AGGREGATIONNODE_H

#include <Interpreters/ExecNode/ExecNode.h>
#include <Core/Block.h>
#include <Interpreters/AggregateDescription.h>


namespace DB {



class AggregationNode :public  ExecNode{


using aggStreamPtr = std::shared_ptr<IBlockInputStream> ;
public:

    // prepare agg param and aggstream
    void prepare() override ;

    void prepareWithMergeable() ;
    void prepareFinal();

    Block  getNext() override;
    Block  getNextWithMergeable();
    Block  getNextFinal();

    void finish() override;

    Block  getHeader() override ;

    Block  getHeaderBeforeAgg();
    Block  getHeaderBeforeFinalMerge();

    void getAggregateInfoFromParam(Names & names,AggregateDescriptions & descriptions);
    void getMergeInfoFromParam(Names & names,AggregateDescriptions & descriptions);


private:

    bool  overflow_row ;
    aggStreamPtr aggStream;
    aggStreamPtr finalStream;
    bool final = false;
    size_t  max_streams ;

    BlockInputStreamPtr stream_with_non_joined_data;



};



}


#endif //CLICKHOUSE_AGGREGATIONNODE_H
