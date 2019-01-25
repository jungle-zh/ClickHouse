//
// Created by admin on 19/1/23.
//

#ifndef CLICKHOUSE_EXPRACTIONBLOCKINPUTSTREAM_H
#define CLICKHOUSE_EXPRACTIONBLOCKINPUTSTREAM_H

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Interpreters/Expr/ExprActions.h>

namespace DB {

class ExprActionBlockInputStream : public IProfilingBlockInputStream
{
using ExprActionsPtr = std::shared_ptr<ExprAction> ;
private:
    using ExpressionActionsPtr = std::shared_ptr<ExprActions>;

public:
    ExprActionBlockInputStream(const BlockInputStreamPtr & input, const ExprActionsPtr & expr_);

    ~ExprActionBlockInputStream(){ }
    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ExprActionsPtr expr ;
};


}


#endif //CLICKHOUSE_EXPRACTIONBLOCKINPUTSTREAM_H
