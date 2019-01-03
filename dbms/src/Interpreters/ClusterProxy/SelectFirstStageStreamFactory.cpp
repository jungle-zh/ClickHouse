#include <Interpreters/ClusterProxy/SelectFirstStageStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>
#include <DataStreams/LazyBlockInputStream.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <common/logger_useful.h>
#include <Parsers/queryToString.h>
#include <Interpreters/InterpreterEnhanceJoinSelectQuery.h>


namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_REPLICAS_ARE_STALE;
}

namespace ClusterProxy
{

SelectFirstStageStreamFactory::SelectFirstStageStreamFactory(
    const Block & header,
    QueryProcessingStage::Enum processed_stage_,
    QualifiedTableName main_table_,
    const Tables & external_tables_)
    : header(header),
    processed_stage{processed_stage_},
    main_table(std::move(main_table_)),
    external_tables{external_tables_}
{
}

/**
namespace
{

BlockInputStreamPtr createLocalStream(const ASTPtr & query_ast, const Context & context, QueryProcessingStage::Enum processed_stage)
{

    //InterpreterSelectQuery interpreter{query_ast, context, {}, processed_stage};
    //BlockInputStreamPtr stream = interpreter.execute().in;

    InterpreterEnhanceJoinSelectQuery  interpreter{query_ast, context, {}, processed_stage, Protocol::Client::FirstStageQuery};
    BlockInputStreamPtr stream = interpreter.execute().in;


    return std::make_shared<MaterializingBlockInputStream>(stream);
}

}
***/
void SelectFirstStageStreamFactory::createForShard(
    const Cluster::ShardInfo & shard_info,
    const String & query, const ASTPtr &,
    const Context & context, const ThrottlerPtr & throttler,
    BlockInputStreams & res)
{
    /**
    auto emplace_local_stream = [&]()
    {
        LOG_DEBUG(&Logger::get("SelectFirstStageStreamFactory"),"add createLocalStream ,query :" + queryToString(query_ast));
        res.emplace_back(createLocalStream(query_ast, context, processed_stage));
    };
     ***/

    LOG_DEBUG(&Logger::get("SelectFirstStageStreamFactory"),"query is " + query);
    auto emplace_remote_stream = [&]()
    {

        LOG_DEBUG(&Logger::get("SelectFirstStageStreamFactory"),"add RemoteBlockInputStream ,query : " + query);

        // query include  main shuffle table name
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, header, context, nullptr, throttler, external_tables, processed_stage,Protocol::Client::FirstStageQuery);
        stream->setPoolMode(PoolMode::GET_MANY);
        //stream->setMainTable(main_table);  do not check main table
        res.emplace_back(std::move(stream));
    };

    if (shard_info.isLocal()) {

        //do nothing

    }
    else{
        LOG_DEBUG(&Logger::get("SelectFirstStageStreamFactory"),"shard is remote ");
        emplace_remote_stream();
    }

}

}
}
