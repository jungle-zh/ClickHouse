#include <Interpreters/ExpressionActions.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <common/logger_useful.h>


namespace DB
{

ExpressionBlockInputStream::ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_)
    : expression(expression_)
{
    children.push_back(input);
}

String ExpressionBlockInputStream::getName() const { return "Expression : expressionActions \n" + expression->dumpActions(); }

Block ExpressionBlockInputStream::getTotals()
{
    if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        expression->executeOnTotals(totals);
    }

    return totals;
}

Block ExpressionBlockInputStream::getHeader() const
{
    //LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"start getHeader");
    Block res = children.back()->getHeader();
    expression->execute(res);
    //LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"end  getHeader");
    return res;
}

Block ExpressionBlockInputStream::readImpl()
{
    LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"start readImpl ,expression: \n " + expression->dumpActions());
    Block res = children.back()->read();
    if (!res)
        return res;
    expression->execute(res);
    LOG_DEBUG(&Logger::get("ExpressionBlockInputStream"),"end readImpl ,expression: \n " + expression->dumpActions());
    return res;
}

}
