//
// Created by admin on 19/1/23.
//

#include <DataStreams/ExprActionBlockInputStream.h>
#include <common/logger_useful.h>


namespace DB
{

    ExprActionBlockInputStream::ExprActionBlockInputStream(const BlockInputStreamPtr & input, const ExprActionsPtr & expression_)
            : expr(expression_)
    {
        children.push_back(input);
    }

    String ExprActionBlockInputStream::getName() const { return "Expression : expressionActions \n" + expr->dumpActions(); }

    Block ExprActionBlockInputStream::getTotals()
    {
        if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
        {
            totals = child->getTotals();
            expr->executeOnTotals(totals);
        }

        return totals;
    }

    Block ExprActionBlockInputStream::getHeader() const
    {
        //LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"start getHeader");
        Block res = children.back()->getHeader();
        expr->execute(res);
        //LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"end  getHeader");
        return res;
    }

    Block ExprActionBlockInputStream::readImpl()
    {
        LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"start readImpl ,expression: \n " + expr->dumpActions());
        Block res = children.back()->read();
        if (!res)
            return res;
        expr->execute(res);
        LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"end readImpl ,expression: \n " + expr->dumpActions());
        return res;
    }

}
