#pragma once

#include <optional>

#include <common/logger_useful.h>

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/Throttler.h>
#include <Interpreters/Context.h>
#include <Client/ConnectionPool.h>
#include <Client/MultiplexedConnections.h>
#include <Interpreters/Cluster.h>

#include <DataStreams/RemoteBlockOutputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include "UnionBlockInputStream.h"

namespace DB
{

/** This class allows one to launch queries on remote replicas of one shard and get results
  */
class FirstStageProxyStream : public IProfilingBlockInputStream
{
public:
    /// Takes already set connection.
    /// If `settings` is nullptr, settings will be taken from context.
    FirstStageProxyStream(
            const BlockInputStreamPtr & input,
            const  JoinPtr &  join,
            const String & shuffleMainTable_,
            const std::shared_ptr<Cluster> & cluster,
            const ASTPtr & query_, const Block & header_, const Context & context_, const Settings * settings = nullptr,
            const Tables & external_tables_ = Tables(),
            QueryProcessingStage::Enum stage_ = QueryProcessingStage::Complete);


    ~FirstStageProxyStream() override;

    /// Specify how we allocate connections on a shard.
    void setPoolMode(PoolMode pool_mode_) { pool_mode = pool_mode_; }

    bool ensureRemoteStorageCreated();

    void setMainTable(QualifiedTableName main_table_) { main_table = std::move(main_table_); }

    /// Besides blocks themself, get blocks' extra info
    void appendExtraInfo();

    /// Sends query (initiates calculation) before read()
    void readPrefix() override;

    /** Prevent default progress notification because progress' callback is
        called by its own
      */
    void progress(const Progress & /*value*/) override {}

    void cancel(bool kill) override;

    String getName() const override { return "FirstStageProxy"; }

    BlockExtraInfo getBlockExtraInfo() const override
    {
        return multiplexed_connections->getBlockExtraInfo();
    }

    Block getHeader() const override { return header; }

    //static void shuffleWrite(Block & block,const  Names & key_names,BlockOutputStreamPtr &  stream,const  Context & context);
protected:
    /// Send all temporary tables to remote servers
    void sendExternalTables();

    Block readImpl() override;

    void readSuffixImpl() override;

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;


private:


    void sendQuery();




    void  createRemoteBlockInputStream(BlockInputStreams & streams);
    void  init();
    Block receiveBlock();

    /// If wasn't sent yet, send request to cancell all connections to replicas
    void tryCancel(const char * reason);

private:
    //const JoinPtr  & join;
    JoinPtr   join;
    const String shuffleMainTable ;


    const std::shared_ptr<Cluster> cluster;
    const ASTPtr modified_query_ast;

    Block header;

    std::function<std::unique_ptr<MultiplexedConnections>()> create_multiplexed_connections;

    std::unique_ptr<MultiplexedConnections> multiplexed_connections;


     Context  context;
    /// Temporary tables needed to be sent to remote servers
    Tables external_tables;
    QueryProcessingStage::Enum stage;

    /// Streams for reading from temporary tables and following sending of data
    /// to remote servers for GLOBAL-subqueries
    std::vector<ExternalTablesData> external_tables_data;
    std::mutex external_tables_mutex;

    /// Connections to replicas are established, but no queries are sent yet
    std::atomic<bool> established { false };

    /// Query is sent (used before getting first block)
    std::atomic<bool> sent_query { false };

    /** All data from all replicas are received, before EndOfStream packet.
      * To prevent desynchronization, if not all data is read before object
      * destruction, it's required to send cancel query request to replicas and
      * read all packets before EndOfStream
      */
    std::atomic<bool> finished { false };

    /** Cancel query request was sent to all replicas beacuse data is not needed anymore
      * This behaviour may occur when:
      * - data size is already satisfactory (when using LIMIT, for example)
      * - an exception was thrown from client side
      */
    std::atomic<bool> was_cancelled { false };

    /** An exception from replica was received. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_exception_from_replica { false };

    /** Unkown packet was received from replica. No need in receiving more packets or
      * requesting to cancel query execution
      */
    std::atomic<bool> got_unknown_packet_from_replica { false };

    bool append_extra_info = false;
    PoolMode pool_mode = PoolMode::GET_MANY;
    std::optional<QualifiedTableName> main_table;

    //std::vector<RemoteBlockOutputStream> streams;

    //BlockOutputStreams out_streams;

    //std::shared_ptr<DistributedBlockOutputStream> out_stream;

    BlockInputStreams  in_streams;


    std::shared_ptr<IBlockInputStream> ustream;

    std::thread shuffle_out_thread;

    bool remote_write_thread_start = false;

   // size_t  key_column_index;

   // size_t  out_num;

    Logger * log = &Logger::get("FirstStageProxyStream");


};

}
