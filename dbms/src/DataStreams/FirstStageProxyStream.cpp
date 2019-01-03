#include <DataStreams/FirstStageProxyStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Common/NetException.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Storages/IStorage.h>
#include <Functions/FunctionFactory.h>
#include <DataStreams/RemoteBlockOutputStream.h>

#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Storages/Distributed/DistributedBlockOutputStream.h>
#include <Interpreters/InterpreterEnhanceJoinSelectQuery.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectFirstStageStreamFactory.h>
#include <Parsers/queryToString.h>
#include "materializeBlock.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}


FirstStageProxyStream::FirstStageProxyStream(
        const BlockInputStreamPtr & input,
        const JoinPtr &  join_,
        const String & shuffleMainTable_,
        const std::shared_ptr<Cluster> & cluster_,
        const ASTPtr & query_, const Block & header_, const Context & context_, const Settings * settings,
        const Tables & external_tables_, QueryProcessingStage::Enum stage_)
    : join(join_),shuffleMainTable(shuffleMainTable_),cluster(cluster_), modified_query_ast(query_), header(header_),context(context_), external_tables(external_tables_), stage(stage_)
{
    if (settings)
        context.setSettings(*settings);
    init();
    children.push_back(input);

    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"create FirstStageProxyStream , join ref:" + std::to_string(join.use_count()));
    /**
    create_multiplexed_connections = [this, &connection, throttler]()
    {
        return std::make_unique<MultiplexedConnections>(connection, context.getSettingsRef(), throttler);
    };
    **/
}



FirstStageProxyStream::~FirstStageProxyStream()
{
    /** If interrupted in the middle of the loop of communication with replicas, then interrupt
      * all connections, then read and skip the remaining packets to make sure
      * these connections did not remain hanging in the out-of-sync state.
      */
    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"~FirstStageProxyStream");
    shuffle_out_thread.join();
    if (established || isQueryPending()){
        LOG_DEBUG(&Logger::get("FirstStageProxyStream")," multiplexed_connections disconnect");
        multiplexed_connections->disconnect();
    }

}

void FirstStageProxyStream::appendExtraInfo()
{
    append_extra_info = true;
}

void FirstStageProxyStream::readPrefix()
{
    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"readPrefix");
    //if (!sent_query)
    //    sendQuery();
}
void FirstStageProxyStream::init(){
    //createRemoteBlockInputStream(in_streams); //
   // createRemoteBlockOutputStream(out_stream); //send table name
    //ustream = std::make_shared<UnionBlockInputStream<>>(in_streams, nullptr,in_streams.size());
}

void FirstStageProxyStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;

    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    {
        std::lock_guard<std::mutex> lock(external_tables_mutex);

        /// Stop sending external data.
        for (auto & vec : external_tables_data)
            for (auto & elem : vec)
                if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(elem.first.get()))
                    stream->cancel(kill);
    }

    if (!isQueryPending() || hasThrownException())
        return;

    tryCancel("Cancelling query");
}

void FirstStageProxyStream::sendExternalTables()
{
    size_t count = multiplexed_connections->size();

    {
        std::lock_guard<std::mutex> lock(external_tables_mutex);

        external_tables_data.reserve(count);

        for (size_t i = 0; i < count; ++i)
        {
            ExternalTablesData res;
            for (const auto & table : external_tables)
            {
                StoragePtr cur = table.second;
                QueryProcessingStage::Enum stage = QueryProcessingStage::Complete;
                BlockInputStreams input = cur->read(cur->getColumns().getNamesOfPhysical(), {}, context,
                    stage, DEFAULT_BLOCK_SIZE, 1);
                if (input.size() == 0)
                    res.push_back(std::make_pair(std::make_shared<OneBlockInputStream>(cur->getSampleBlock()), table.first));
                else
                    res.push_back(std::make_pair(input[0], table.first));
            }
            external_tables_data.push_back(std::move(res));
        }
    }

    multiplexed_connections->sendExternalTablesData(external_tables_data);
}


/** If we receive a block with slightly different column types, or with excessive columns,
  *  we will adapt it to expected structure.
  */
  /*
static Block adaptBlockStructure(const Block & block, const Block & header, const Context & context)
{
    /// Special case when reader doesn't care about result structure. Deprecated and used only in Benchmark, PerformanceTest.
    if (!header)
        return block;

    Block res;
    res.info = block.info;

    for (const auto & elem : header)
        res.insert({ castColumn(block.getByName(elem.name), elem.type, context), elem.type, elem.name });
    return res;
}   */






void FirstStageProxyStream::createRemoteBlockInputStream(BlockInputStreams & streams) { // after send  out



    // shuffleMainTable should be contained in modified_query_ast
    //Block header = materializeBlock(InterpreterEnhanceJoinSelectQuery(modified_query_ast, context, {},  QueryProcessingStage::WithMergeableState, Protocol::Client::FirstStageQuery).getSampleBlock());

      LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"select_stream_factory");
    ClusterProxy::SelectFirstStageStreamFactory select_stream_factory(
            header, QueryProcessingStage::WithMergeableState, QualifiedTableName{"", shuffleMainTable}, context.getExternalTables());


      LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"ClusterProxy::executeQuery");

    const Settings & settings = context.getSettingsRef();
    streams = ClusterProxy::executeQuery(
            select_stream_factory, cluster, modified_query_ast, context, settings);




}




Block FirstStageProxyStream::readImpl()
{

    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"readImpl ");

    if(!remote_write_thread_start){


        //shuffleMainTable from subquery  or origin column stream , dont write to local file
       // dont write to local
       // BlockOutputStreamPtr out_stream =  std::make_shared<DistributedBlockOutputStream>(nullptr,cluster,context.getSettings(), false,0,Protocol::Client::ShuffleWriteMainTable,shuffleMainTable); error
        shuffle_out_thread = std::thread (
            [&] () {
                BlockOutputStreamPtr out_stream =  std::make_shared<DistributedBlockOutputStream>(nullptr,cluster,context.getSettings(), false,0,Protocol::Client::ShuffleWriteMainTable,shuffleMainTable);
                LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"in thread ......");
                Block head ;
                bool  getHeader  = false;

                while(Block block = children.back()->read()){    // child already union  ,

                    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"read block size :" + std::to_string(block.rows()));
                    if(!getHeader){
                        head = block.cloneEmpty();
                        getHeader = true;
                    }
                    context.getCurrentQueryId();
                    join->getLeftTableKeyName();
                    dynamic_cast<DistributedBlockOutputStream* > (out_stream.get())->shuffleWrite(block,join->getLeftTableKeyName(),context);
                }

                LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"main table writeSuffix ");
                //Block empty_block;
                //dynamic_cast<DistributedBlockOutputStream* > (out_stream.get())->shuffleWrite(empty_block,join->getLeftTableKeyName(),context);
                dynamic_cast<DistributedBlockOutputStream* > (out_stream.get())->writeSuffixForce(); // will not diconnect  until waiting   remote  send eof package
           } );

        remote_write_thread_start = true;
    }



    if(!ustream){

        LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"createRemoteBlockInputStream");
        createRemoteBlockInputStream(in_streams); // before remoteInputStream send request ,need confirm outStream packet already send and StorageMemoryFifo already create in  remote

        if(ensureRemoteStorageCreated()){
            ustream = std::make_shared<UnionBlockInputStream<>>(in_streams, nullptr,in_streams.size());
        } else{

            throw  Exception("remote storage not created");
        }
    }


    return  ustream->read();

   // return header;


}


bool FirstStageProxyStream::ensureRemoteStorageCreated(){

    bool res = true;

    LOG_DEBUG(&Logger::get("FirstStageProxyStream"),"ensureRemoteStorageCreated");
    for(auto & e : in_streams){

        RemoteBlockInputStream  * input_stream =  dynamic_cast<RemoteBlockInputStream *>(e.get());

        res &= input_stream->askIfShuffleStorageBuild(shuffleMainTable);

    }

    return res;

}
void FirstStageProxyStream::readSuffixImpl()
{
    /** If one of:
      * - nothing started to do;
      * - received all packets before EndOfStream;
      * - received exception from one replica;
      * - received an unknown packet from one replica;
      * then you do not need to read anything.
      */
    if (!isQueryPending() || hasThrownException())
        return;

    /** If you have not read all the data yet, but they are no longer needed.
      * This may be due to the fact that the data is sufficient (for example, when using LIMIT).
      */

    /// Send the request to abort the execution of the request, if not already sent.
    tryCancel("Cancelling query because enough data has been read");

    /// Get the remaining packets so that there is no out of sync in the connections to the replicas.
    Connection::Packet packet = multiplexed_connections->drain();
    switch (packet.type)
    {
        case Protocol::Server::EndOfStream:
            finished = true;
            break;

        case Protocol::Server::Exception:
            got_exception_from_replica = true;
            packet.exception->rethrow();
            break;

        default:
            got_unknown_packet_from_replica = true;
            throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
}

void FirstStageProxyStream::sendQuery()
{
    /*
    multiplexed_connections = create_multiplexed_connections();

    LOG_DEBUG(&Logger::get("FirstStageProxyStream"), "send query :" + queryToString(modified_query_ast) + " to dumpAddress:" + multiplexed_connections->dumpAddresses());

    if (context.getSettingsRef().skip_unavailable_shards && 0 == multiplexed_connections->size())
        return;

    established = true;

    multiplexed_connections->sendQuery(queryToString(modified_query_ast), "", stage, &context.getClientInfo(), true);

    established = false;
    sent_query = true;

    sendExternalTables();
     */
}

void FirstStageProxyStream::tryCancel(const char * reason)
{
    bool old_val = false;
    if (!was_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    LOG_TRACE(log, "(" << multiplexed_connections->dumpAddresses() << ") " << reason);
    multiplexed_connections->sendCancel();
}

bool FirstStageProxyStream::isQueryPending() const
{
    return sent_query && !finished;
}

bool FirstStageProxyStream::hasThrownException() const
{
    return got_exception_from_replica || got_unknown_packet_from_replica;
}

}
