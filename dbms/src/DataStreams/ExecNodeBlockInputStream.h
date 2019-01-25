//
// Created by admin on 19/1/23.
//

#ifndef CLICKHOUSE_EXECNODEBLOCKINPUTSTREAM_H
#define CLICKHOUSE_EXECNODEBLOCKINPUTSTREAM_H

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {



class ExecNodeBlockInputStream : public IProfilingBlockInputStream
{
using ExecNodePtr = std::shared_ptr<ExecNode>   ;
private:


public:
    ExecNodeBlockInputStream( const ExecNodePtr & node );

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ExecNodePtr node ;
    Block header;


};


}

#endif //CLICKHOUSE_EXECNODEBLOCKINPUTSTREAM_H
