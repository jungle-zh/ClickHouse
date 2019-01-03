#include <Interpreters/Set.h>
#include <Interpreters/Join.h>
#include <DataStreams/materializeBlock.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Storages/IStorage.h>
#include <iomanip>
#include "FirstStageProxyStream.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}


    CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
            const BlockInputStreamPtr & input,
            const SubqueriesForSets & subqueries_for_sets_,
            const SizeLimits & network_transfer_limits
            )
            : subqueries_for_sets(subqueries_for_sets_),
              network_transfer_limits(network_transfer_limits)
    {
        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "CreatingSetsBlockInputStream constuct ");
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source)
            {
                std::stringstream log_str;
                elem.second.source->dumpTree(log_str);
                LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream")," add subquery :" + elem.first + " of  stream :\n" + log_str.str());
                children.push_back(elem.second.source);

                if (elem.second.set)
                    elem.second.set->setHeader(elem.second.source->getHeader());
            }
        }

        children.push_back(input);
        std::stringstream log_str;
        input->dumpTree(log_str);
        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream")," add stream  :\n" + log_str.str() );
    }

CreatingSetsBlockInputStream::CreatingSetsBlockInputStream(
    const BlockInputStreamPtr & input,
    const SubqueriesForSets & subqueries_for_sets_,
    const SizeLimits & network_transfer_limits,
    const Context & context_)
    : subqueries_for_sets(subqueries_for_sets_),
    network_transfer_limits(network_transfer_limits),
    context(&context_)
{
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "CreatingSetsBlockInputStream constuct ");
    for (auto & elem : subqueries_for_sets)
    {
        if (elem.second.source)
        {
            std::stringstream log_str;
            elem.second.source->dumpTree(log_str);
            LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream")," add subquery :" + elem.first + " of  stream :\n" + log_str.str());
            children.push_back(elem.second.source);

            if (elem.second.set)
                elem.second.set->setHeader(elem.second.source->getHeader());
        }
    }

    children.push_back(input);
    std::stringstream log_str;
    input->dumpTree(log_str);
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream")," add stream  :\n" + log_str.str() );
}


Block CreatingSetsBlockInputStream::readImpl()
{
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , " start  readImpl ");
    Block res;

    createAll();  //create hash table first

    if (isCancelledOrThrowIfKilled())
        return res;
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "  createAll done   ");

    std::stringstream log_str;
    children.back()->dumpTree(log_str);
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "  child stream  :\n " + log_str.str() );
    return children.back()->read(); // child stream in pipeline
}


void CreatingSetsBlockInputStream::readPrefixImpl()
{
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "  readPrefixImpl   ");
    createAll();
}


Block CreatingSetsBlockInputStream::getTotals()
{
    auto input = dynamic_cast<IProfilingBlockInputStream *>(children.back().get());

    if (input)
        return input->getTotals();
    else
        return totals;
}


void CreatingSetsBlockInputStream::createAll()
{
    if (!created)
    {
        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"subqueries_for_sets size is " + std::to_string(subqueries_for_sets.size()) );
        for (auto & elem : subqueries_for_sets)
        {
            if (elem.second.source) /// There could be prepared in advance Set/Join - no source is specified for them.
            {
                LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"subqueries_for_sets key :" + elem.first + " start to createOne");
                if (isCancelledOrThrowIfKilled())
                    return;

                createOne(elem.second);
            }
        }

        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"all createOne finish ");
        created = true;
    } else{
        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream") , "in createAll, all create done ");
    }
}



void CreatingSetsBlockInputStream::createOne(SubqueryForSet & subquery)
{

    std::stringstream log_str;
    if(subquery.source){
        subquery.source->dumpTree(log_str);
    }
    LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"subquery source \n:" + log_str.str() );
    LOG_TRACE(log, (subquery.set ? "start Creating set... " : "")
        << (subquery.join ? "start Creating join...  "    : "")
        << (subquery.table ? "start Filling temporary table... "  + subquery.table->getName() : ""));
    Stopwatch watch;

    BlockOutputStreamPtr table_out;
    if (subquery.table)
        table_out = subquery.table->write({}, {});

    bool done_with_set = !subquery.set;
    bool done_with_join = !subquery.join;
    bool done_with_table = !subquery.table;
   // bool done_with_stream = !subquery.out_stream; // for master node , write shuffle right table

    if (done_with_set && done_with_join && done_with_table)
        throw Exception("Logical error: nothing to do with subquery", ErrorCodes::LOGICAL_ERROR);

    if (table_out)
        table_out->writePrefix();

    bool  getHeader = false;
    Block head ;

    while (Block block = subquery.source->read()) //jungle comment:read all the block ?
    {
        if(!getHeader){
            head = block.cloneEmpty();
            getHeader = true;
        }

        if (isCancelled())
        {
            LOG_DEBUG(log, "Query was cancelled during set / join or temporary table creation.");
            return;
        }

        if (!done_with_set)
        {
            if (!subquery.set->insertFromBlock(block, /*fill_set_elements=*/false))
                done_with_set = true;
        }

        if (!done_with_join)
        {
            if(!subquery.is_shuffle_join){
                if (!subquery.join->insertFromBlock(block))
                    done_with_join = true;
            } else {

                LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"shuffleWrite , block size :" + std::to_string(block.rows()) + ", join ref:" + std::to_string(subquery.join.use_count()));
                dynamic_cast<DistributedBlockOutputStream* > (subquery.out_stream.get())->shuffleWrite(block,subquery.join->getRightTableKeyName(),*context);
            }

        }

        if (!done_with_table)
        {
            block = materializeBlock(block);
            table_out->write(block);

            rows_to_transfer += block.rows();
            bytes_to_transfer += block.bytes();

            if (!network_transfer_limits.check(rows_to_transfer, bytes_to_transfer, "IN/JOIN external table", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
                done_with_table = true;
        }


        if (done_with_set && done_with_join && done_with_table)
        {
            if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
                profiling_in->cancel(false);

            break;
        }
    }

    if(subquery.is_shuffle_join){
        LOG_DEBUG(&Logger::get("CreatingSetsBlockInputStream"),"right table writeSuffix");
        //Block empty_block ;
        //dynamic_cast<DistributedBlockOutputStream* > (subquery.out_stream.get())->shuffleWrite(empty_block,subquery.join->getRightTableKeyName(),*context);
        dynamic_cast<DistributedBlockOutputStream* > (subquery.out_stream.get())->writeSuffixForce();
    }

    if (table_out)
        table_out->writeSuffix();

    watch.stop();

    size_t head_rows = 0;
    if (IProfilingBlockInputStream * profiling_in = dynamic_cast<IProfilingBlockInputStream *>(&*subquery.source))
    {
        const BlockStreamProfileInfo & profile_info = profiling_in->getProfileInfo();

        head_rows = profile_info.rows;

        if (subquery.join)
            subquery.join->setTotals(profiling_in->getTotals());
    }

    if (head_rows != 0)
    {
        std::stringstream msg;
        msg << std::fixed << std::setprecision(3);
        msg << "finish Created... ";

        if (subquery.set)
            msg << "Set with " << subquery.set->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.join)
            msg << "Join with " << subquery.join->getTotalRowCount() << " entries from " << head_rows << " rows. ";
        if (subquery.table)
            msg << "Table with " << head_rows << " rows. ";

        msg << "In " << watch.elapsedSeconds() << " sec.";
        LOG_DEBUG(log, msg.rdbuf());
    }
    else
    {
        LOG_DEBUG(log, "Subquery has empty result.");
    }

    LOG_DEBUG(log, "finish Created  !!");
}

}
