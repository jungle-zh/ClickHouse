#include <iomanip>

#include <Poco/Net/NetException.h>

#include <Common/ClickHouseRevision.h>

#include <Common/Stopwatch.h>

#include <IO/Progress.h>

#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/CompressionSettings.h>

#include <IO/copyData.h>

#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Quota.h>
#include <Interpreters/TablesStatus.h>

#include <Storages/StorageMemoryFifo.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Common/ExternalTable.h>

#include "TCPHandlerForFirstStageExec.h"

#include <Common/NetException.h>
#include <ext/scope_guard.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int SOCKET_TIMEOUT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}


void TCPHandlerForFirstStageExec::waitShuffleStorageConsumed(StorageMemoryFifo *  storageMemoryFifo){

    //assert(storageMemoryFifo->writeDone());

    while (! storageMemoryFifo->readDone()) {

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"), "wait shuffle storeage consumed");
    }
    main_table_read_thread.join();
    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"), "shuffle storeage all  consumed ..");


}

void TCPHandlerForFirstStageExec::runImpl()
{
    connection_context = server.context();
    connection_context.setSessionContext(connection_context);

    Settings global_settings = connection_context.getSettings();

    socket().setReceiveTimeout(global_settings.receive_timeout);
    socket().setSendTimeout(global_settings.send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());

    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    try
    {
        receiveHello();
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        {
            LOG_DEBUG(log, "Client has connected to wrong port.");
            return;
        }

        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_WARNING(log, "Client has gone away.");
            return;
        }

        try
        {
            /// We try to send error information to the client.
            sendException(e);
        }
        catch (...) {}

        throw;
    }

    /// When connecting, the default database can be specified.
    if (!default_database.empty())
    {
        if (!connection_context.isDatabaseExist(default_database))
        {
            Exception e("Database " + default_database + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            LOG_ERROR(log, "Code: " << e.code() << ", e.displayText() = " << e.displayText()
                << ", Stack trace:\n\n" << e.getStackTrace().toString());
            sendException(e);
            return;
        }

        connection_context.setCurrentDatabase(default_database);
    }

    sendHello();

    connection_context.setProgressCallback([this] (const Progress & value) { return this->updateProgress(value); });

    while (1)
    {
        /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) && !server.isCancelled())
            ;

        /// If we need to shut down, or client disconnects.
        if (server.isCancelled() || in->eof())
            break;

        Stopwatch watch;
        state.reset();

        /** An exception during the execution of request (it must be sent over the network to the client).
         *  The client will be able to accept it, if it did not happen while sending another packet and the client has not disconnected yet.
         */
        std::unique_ptr<Exception> exception;
        bool network_error = false;

        try
        {
            /// Restore context of request.
            query_context = connection_context;  // global context ;

            /// If a user passed query-local timeouts, reset socket to initial state at the end of the query
            SCOPE_EXIT({state.timeout_setter.reset();});

            /** If Query - process it. If Ping or Cancel - go back to the beginning.
             *  There may come settings for a separate query that modify `query_context`.
             */
            Protocol::Client::Enum  packet_type ;
            std::string external_table_name ;
            if (!receivePacket(packet_type,external_table_name))
                continue;
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," receive head packet");

            if(packet_type == Protocol::Client::ShuffleWriteMainTable) {
                readShuffleMainTableDataInSeparatedThread(global_settings);

                LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"readShuffleMainTableDataInSeparatedThread 33");


                std::string tables_s;
                Tables tables = server.getGlobalShuffleTable();
                for(auto & t : tables){
                    tables_s +=  t.first;
                    tables_s += ",";
                }
                LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"main table in server.context :" + tables_s);

            }else if (packet_type == Protocol::Client::ShuffleWriteRightTable ){

                readShuffleRightTable(global_settings);  //read all right table data
                state.block_in.reset();
                state.maybe_compressed_in.reset();

            } else if(packet_type == Protocol::Client::AskIfStorageBuild) {

                LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," query is AskIfStorageBuild table name :" + external_table_name);
               // LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," 1 state.shuffle_main_table use count" + std::to_string(state.shuffle_main_table.use_count()));
               //  server.getGlobalShuffleTable()[external_table_name];
               // LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," 4 state.shuffle_main_table use count " + std::to_string(state.shuffle_main_table.use_count()));

               // LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," 5 shu_main_table in server use count " + std::to_string(server.getGlobalShuffleTable()[external_table_name].use_count()) );
                std::string tables_s;
                Tables tables = server.getGlobalShuffleTable();
                for(auto & t : tables){
                    tables_s +=  t.first;
                    tables_s += ",";
                }
                LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"current table in server.context:" + tables_s);

            } else {

                readData(global_settings);
                /// Reset the input stream, as we received an empty block while receiving external table data.
                /// So, the stream has been marked as cancelled and we can't read from it anymore.
                state.block_in.reset();
                state.maybe_compressed_in.reset();  /// For more accurate accounting by MemoryTracker.
            }


            /// Processing Query
             if(  packet_type == Protocol::Client::Query
               || packet_type == Protocol::Client::FirstStageQuery
               || packet_type == Protocol::Client::OriginPullQuery
               || packet_type == Protocol::Client::ShuffleJoinMasterQuery){


                String left_table ;
                String right_table ;

                if(packet_type == Protocol::Client::FirstStageQuery){


                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"receive FirstStageQuery ,query is  " + state.query + " ,check required tables " );
                    //shuffle  table name is in  query
                    //check left table and right table are  all build


                    String query = state.query;
                    ParserQuery parser(query.data() + query.size());
                    ASTPtr  astptr = parseQuery(parser, query.data(), query.data() + query.size(), "", 0);

                    const ASTSelectWithUnionQuery & ast = typeid_cast<const ASTSelectWithUnionQuery &>(*astptr);

                    ASTSelectQuery * selectQuery = dynamic_cast<ASTSelectQuery * > (ast.list_of_selects->children.at(0).get());


                    String left_table_ = queryToString(selectQuery->table()) ;

                    ASTTableExpression *  tableExpression =   static_cast<ASTTableExpression *>( selectQuery->join()->table_expression.get());

                    String right_table_ = queryToString(tableExpression->database_and_table_name);


                    for(auto & c : left_table_){
                        if(c!= '`'){
                            left_table  += c;
                        } else{
                            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"char is ` "  );
                        }
                    }

                    for(auto & c : right_table_){
                        if(c!= '`'){
                            right_table  += c;
                        } else{
                            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"char is ` "  );
                        }
                    }


                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"left table is " + left_table);
                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"right  table is " + right_table);

                    auto ltable = server.getGlobalShuffleTable().find(left_table);
                    auto rtable = server.getGlobalShuffleTable().find(right_table);

                    std::string tables_s;
                    Tables tables = server.getGlobalShuffleTable();
                    for(auto & t : tables){
                        tables_s +=  t.first;
                        tables_s += ",";
                    }
                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"current table in server:" + tables_s);

                    if(ltable == server.getGlobalShuffleTable().end() ){
                        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," left table is not  ready");
                        throw  Exception("left table not ready");
                    }
                    if(rtable == server.getGlobalShuffleTable().end() ){
                        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," right table is not  ready");
                        throw  Exception("right table not ready");
                    }



                }

                state.io = executeQuery(state.query, query_context, false, state.stage,packet_type,std::make_shared<std::map<std::string,StoragePtr>>(server.getGlobalShuffleTable())); // physical plan
                if (state.io.out)
                    state.need_receive_data_for_insert = true;

                after_check_cancelled.restart();
                after_send_progress.restart();

                /// Does the request require receive data from client?
                if (state.need_receive_data_for_insert)
                    processInsertQuery(global_settings);
                else
                    processOrdinaryQuery();


                 if(packet_type == Protocol::Client::FirstStageQuery){

                     server.getGlobalShuffleTable().erase(left_table);
                     server.getGlobalShuffleTable().erase(right_table);


                     std::string tables_s;
                     Tables tables = server.getGlobalShuffleTable();
                     for(auto & t : tables){
                         tables_s +=  t.first;
                         tables_s += ",";
                     }
                     LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"after clear ,table in server:" + tables_s);
                 }


            } else if(packet_type == Protocol::Client::ShuffleWriteMainTable){

                 LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"main table " +  external_table_name + " is  building  ,wait to be consumed :");

                 std::string tables_s;
                 Tables tables = server.getGlobalShuffleTable();
                 for(auto & t : tables){
                     tables_s +=  t.first;
                     tables_s += ",";
                 }
                 LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"current table in server.context:" + tables_s);

                 auto it = server.getGlobalShuffleTable().find(external_table_name);
                 if(it != server.getGlobalShuffleTable().end()){
                     state.shuffle_main_table = it->second;
                     StoragePtr storage = it->second;           // another tcphandler receive query and read from external table
                     waitShuffleStorageConsumed(dynamic_cast<StorageMemoryFifo *> (&*storage));

                 } else{
                     LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"not find storage ");
                 }



            } else if(packet_type == Protocol::Client::ShuffleWriteRightTable){

               //  StoragePtr storage = server.getGlobalShuffleTable()[external_table_name]; // right table build in memory
                 auto it = server.getGlobalShuffleTable().find(external_table_name);
                 if(it != server.getGlobalShuffleTable().end()){
                     StoragePtr storage = it->second;
                     StorageMemory & storageMemory = static_cast<StorageMemory &>( *storage.get());
                     LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"right table "  + external_table_name +  " build done ,size is " +  std::to_string(storageMemory.getSize())  );

                 }else {

                     std::string tables_s;
                     Tables tables = server.getGlobalShuffleTable();
                     for(auto & t : tables){
                         tables_s +=  t.first;
                         tables_s += ",";
                     }

                     LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"right table " + external_table_name + " not find in global shuffle table , current tables :" + tables_s  );

                 }

                 //do not need to wait
            } else if(packet_type == Protocol::Client::AskIfStorageBuild){  // in send query connection

                size_t max_try = 10 ;
                while(!state.shuffle_main_table && max_try > 0 ){
                    max_try -- ;
                    usleep(1000 * 100);
                    auto it = server.getGlobalShuffleTable().find(external_table_name);
                    if(it != server.getGlobalShuffleTable().end()){
                        state.shuffle_main_table = it->second;
                    }


                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," wait shuffle_main_table ");

                    std::string tables_s;
                    Tables tables = server.getGlobalShuffleTable();
                    for(auto & t : tables){
                        tables_s +=  t.first;
                        tables_s += ",";
                    }
                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"current table in server.context:" + tables_s);

                }

                writeVarUInt(Protocol::Server::ResponseIfStorageBuild, *out);

                if(state.shuffle_main_table){
                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," ResponseIfStorageBuild yes");
                    writeStringBinary("yes", *out);
                } else {
                    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," ResponseIfStorageBuild no");
                    writeStringBinary("no", *out);
                }


            }

            sendEndOfStream();
            state.reset();
        }
        catch (const Exception & e)
        {
            state.io.onException();
            exception.reset(e.clone());

            if (e.code() == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
                throw;

            /// If a timeout occurred, try to inform client about it and close the session
            if (e.code() == ErrorCodes::SOCKET_TIMEOUT)
                network_error = true;
        }
        catch (const Poco::Net::NetException & e)
        {
            /** We can get here if there was an error during connection to the client,
             *  or in connection with a remote server that was used to process the request.
             *  It is not possible to distinguish between these two cases.
             *  Although in one of them, we have to send exception to the client, but in the other - we can not.
             *  We will try to send exception to the client in any case - see below.
             */
            state.io.onException();
            exception = std::make_unique<Exception>(e.displayText(), ErrorCodes::POCO_EXCEPTION);
        }
        catch (const Poco::Exception & e)
        {
            state.io.onException();
            exception = std::make_unique<Exception>(e.displayText(), ErrorCodes::POCO_EXCEPTION);
        }
        catch (const std::exception & e)
        {
            state.io.onException();
            exception = std::make_unique<Exception>(e.what(), ErrorCodes::STD_EXCEPTION);
        }
        catch (...)
        {
            state.io.onException();
            exception = std::make_unique<Exception>("Unknown exception", ErrorCodes::UNKNOWN_EXCEPTION);
        }

        try
        {
            if (exception)
                sendException(*exception);
        }
        catch (...)
        {
            /** Could not send exception information to the client. */
            network_error = true;
            LOG_WARNING(log, "Client has gone away.");
        }

        try
        {
            state.reset();
        }
        catch (...)
        {
            /** During the processing of request, there was an exception that we caught and possibly sent to client.
             *  When destroying the request pipeline execution there was a second exception.
             *  For example, a pipeline could run in multiple threads, and an exception could occur in each of them.
             *  Ignore it.
             */
        }

        watch.stop();

        LOG_INFO(log, std::fixed << std::setprecision(3)
            << "Processed in " << watch.elapsedSeconds() << " sec.");

        if (network_error)
            break;
    }
}

void TCPHandlerForFirstStageExec::readShuffleMainTableDataInSeparatedThread(const Settings & global_settings){

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"readShuffleMainTableDataInSeparatedThread 11");
    main_table_read_thread = std::thread (&TCPHandlerForFirstStageExec::readData,this,global_settings);

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"readShuffleMainTableDataInSeparatedThread 22");

}

void TCPHandlerForFirstStageExec::readShuffleRightTable(const Settings & global_settings){


    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"readShuffleRightTable");
    readData(global_settings);

}





void TCPHandlerForFirstStageExec::readData(const Settings & global_settings)
{
    auto receive_timeout = query_context.getSettingsRef().receive_timeout.value;

    /// Poll interval should not be greater than receive_timeout
    size_t default_poll_interval = global_settings.poll_interval.value * 1000000;
    size_t current_poll_interval = static_cast<size_t>(receive_timeout.totalMicroseconds());
    constexpr size_t min_poll_interval = 5000; // 5 ms
    size_t poll_interval = std::max(min_poll_interval, std::min(default_poll_interval, current_poll_interval));

    while (1)
    {
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
        while (1)
        {
            if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(poll_interval))
                break;

            /// Do we need to shut down?
            if (server.isCancelled())
                return;

            /** Have we waited for data for too long?
             *  If we periodically poll, the receive_timeout of the socket itself does not work.
             *  Therefore, an additional check is added.
             */
            double elapsed = watch.elapsedSeconds();
            if (elapsed > receive_timeout.totalSeconds())
            {
                std::stringstream ss;
                ss << "Timeout exceeded while receiving data from client.";
                ss << " Waited for " << static_cast<size_t>(elapsed) << " seconds,";
                ss << " timeout is " << receive_timeout.totalSeconds() << " seconds.";

                throw Exception(ss.str(), ErrorCodes::SOCKET_TIMEOUT);
            }
        }

        /// If client disconnected.
        if (in->eof()){

            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"client stream is eof ");
            return;
        }


        /// We accept and process data. And if they are over, then we leave.
        Protocol::Client::Enum   packet_type ;
        std::string external_table_name;
        if (!receivePacket(packet_type,external_table_name))
            break;
    }
}


void TCPHandlerForFirstStageExec::processInsertQuery(const Settings & global_settings)
{
    /** Made above the rest of the lines, so that in case of `writePrefix` function throws an exception,
      *  client receive exception before sending data.
      */
    state.io.out->writePrefix();

    /// Send block to the client - table structure.
    Block block = state.io.out->getHeader();
    sendData(block);

    readData(global_settings);
    state.io.out->writeSuffix();
    state.io.onFinish();
}


void TCPHandlerForFirstStageExec::processOrdinaryQuery()
{

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"start processOrdinaryQuery\n");
    /// Pull query execution result, if exists, and send it to network.
    if (state.io.in)
    {
        /// Send header-block, to allow client to prepare output format for data to send.
        {
            Block header = state.io.in->getHeader();
            if (header)
                sendData(header);
        }

        AsynchronousBlockInputStream async_in(state.io.in);
        async_in.readPrefix();
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"), "readPrefix done");

        while (true)
        {
            Block block;

            while (true)
            {
                if (isQueryCancelled())
                {
                    /// A packet was received requesting to stop execution of the request.
                    async_in.cancel(false);
                    break;
                }
                else
                {
                    if (state.progress.rows && after_send_progress.elapsed() / 1000 >= query_context.getSettingsRef().interactive_delay)
                    {
                        /// Some time passed and there is a progress.
                        after_send_progress.restart();
                        sendProgress();
                    }

                    if (async_in.poll(query_context.getSettingsRef().interactive_delay / 1000))
                    {
                        /// There is the following result block.
                        block = async_in.read();
                        break;
                    }
                }
            }

            /** If data has run out, we will send the profiling data and total values to
              * the last zero block to be able to use
              * this information in the suffix output of stream.
              * If the request was interrupted, then `sendTotals` and other methods could not be called,
              *  because we have not read all the data yet,
              *  and there could be ongoing calculations in other threads at the same time.
              */
            if (!block && !isQueryCancelled())
            {
                sendTotals();
                sendExtremes();
                sendProfileInfo();
                sendProgress();
            }

            sendData(block);
            if (!block){
                LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"), "block all sends to client , job finished ");
                break;
            }

        }

        async_in.readSuffix();
    }

    state.io.onFinish();
}


void TCPHandlerForFirstStageExec::processTablesStatusRequest()
{
    TablesStatusRequest request;
    request.read(*in, client_revision);

    TablesStatusResponse response;
    for (const QualifiedTableName & table_name: request.tables)
    {
        StoragePtr table = connection_context.tryGetTable(table_name.database, table_name.table);
        if (!table)
            continue;

        TableStatus status;
        if (auto * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get()))
        {
            status.is_replicated = true;
            status.absolute_delay = replicated_table->getAbsoluteDelay();
        }
        else
            status.is_replicated = false;

        response.table_states_by_id.emplace(table_name, std::move(status));
    }

    writeVarUInt(Protocol::Server::TablesStatusResponse, *out);
    response.write(*out, client_revision);
}


void TCPHandlerForFirstStageExec::sendProfileInfo()
{
    if (const IProfilingBlockInputStream * input = dynamic_cast<const IProfilingBlockInputStream *>(state.io.in.get()))
    {
        writeVarUInt(Protocol::Server::ProfileInfo, *out);
        input->getProfileInfo().write(*out);
        out->next();
    }
}


void TCPHandlerForFirstStageExec::sendTotals()
{
    if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(state.io.in.get()))
    {
        const Block & totals = input->getTotals();

        if (totals)
        {
            initBlockOutput(totals);

            writeVarUInt(Protocol::Server::Totals, *out);
            writeStringBinary("", *out);

            state.block_out->write(totals);
            state.maybe_compressed_out->next();
            out->next();
        }
    }
}


void TCPHandlerForFirstStageExec::sendExtremes()
{
    if (IProfilingBlockInputStream * input = dynamic_cast<IProfilingBlockInputStream *>(state.io.in.get()))
    {
        Block extremes = input->getExtremes();

        if (extremes)
        {
            initBlockOutput(extremes);

            writeVarUInt(Protocol::Server::Extremes, *out);
            writeStringBinary("", *out);

            state.block_out->write(extremes);
            state.maybe_compressed_out->next();
            out->next();
        }
    }
}


void TCPHandlerForFirstStageExec::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;
    String user = "default";
    String password;

    readVarUInt(packet_type, *in);
    if (packet_type != Protocol::Client::Hello)
    {
        /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
          * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
          */
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString("HTTP/1.0 400 Bad Request\r\n\r\n"
                "Port " + server.config().getString("tcp_port") + " is for clickhouse-client program.\r\n"
                "You must use port " + server.config().getString("http_port") + " for HTTP.\r\n",
                *out);

            throw Exception("Client has connected to wrong port", ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
        }
        else
            throw NetException("Unexpected packet from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    readVarUInt(client_revision, *in);
    readStringBinary(default_database, *in);
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    LOG_DEBUG(log, "Connected " << client_name
        << " version " << client_version_major
        << "." << client_version_minor
        << "." << client_revision
        << (!default_database.empty() ? ", database: " + default_database : "")
        << (!user.empty() ? ", user: " + user : "")
        << ".");

    connection_context.setUser(user, password, socket().peerAddress(), "");
}


void TCPHandlerForFirstStageExec::sendHello()
{
    writeVarUInt(Protocol::Server::Hello, *out);
    writeStringBinary(DBMS_NAME, *out);
    writeVarUInt(DBMS_VERSION_MAJOR, *out);
    writeVarUInt(DBMS_VERSION_MINOR, *out);
    writeVarUInt(ClickHouseRevision::get(), *out);
    if (client_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
    {
        writeStringBinary(DateLUT::instance().getTimeZone(), *out);
    }
    if (client_revision >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
    {
        writeStringBinary(server_display_name, *out);
    }
    out->next();
}


bool TCPHandlerForFirstStageExec::receivePacket(Protocol::Client::Enum & packet_type_ ,std::string & external_table_name  )
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"... receivePacket :" + std::to_string(packet_type));

    packet_type_ = static_cast<Protocol::Client::Enum>(packet_type);
//    std::cerr << "Packet: " << packet_type << std::endl;

    switch (packet_type_)
    {
        case Protocol::Client::Query:
        case Protocol::Client::OriginPullQuery:
        case Protocol::Client::FirstStageQuery:
        case Protocol::Client::ShuffleJoinMasterQuery:
            if (!state.empty())
                throw NetException("Unexpected packet Query received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            receiveQuery();
            return true;

        case Protocol::Client::Data:
            if (state.empty())
                throw NetException("Unexpected packet Data received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            return receiveData();
        case Protocol::Client::ShuffleWriteMainTable:
            if( !state.empty())
                throw NetException("Unexpected packet Query received from client for ShuffleData" , ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            return  receiveShuffleMainTable(external_table_name);


        case Protocol::Client::ShuffleWriteRightTable:
            if( !state.empty())
                throw NetException("Unexpected packet Query received from client for ShuffleData" , ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            return receiveShuffleRightTable(external_table_name);


        case Protocol::Client::AskIfStorageBuild:
            if( !state.empty())
                throw NetException("Unexpected packet Query received from client for ShuffleData" , ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            receiveAskIfStorageBuild(external_table_name);
            return true;

        case Protocol::Client::Ping:
            writeVarUInt(Protocol::Server::Pong, *out);
            out->next();
            return false;

        case Protocol::Client::Cancel:
            return false;

        case Protocol::Client::Hello:
            throw Exception("Unexpected packet " + String(Protocol::Client::toString(packet_type)) + " received from client",
                ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

        case Protocol::Client::TablesStatusRequest:
            if (!state.empty())
                throw NetException("Unexpected packet TablesStatusRequest received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            processTablesStatusRequest();
            out->next();
            return false;

        default:
            throw Exception("Unknown packet " + toString(packet_type) + " from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
}


void TCPHandlerForFirstStageExec::receiveQuery()
{
    UInt64 stage = 0;
    UInt64 compression = 0;

    state.is_empty = false;
    readStringBinary(state.query_id, *in);

    query_context.setCurrentQueryId(state.query_id);

    /// Client info
    {
        ClientInfo & client_info = query_context.getClientInfo();
        if (client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
            client_info.read(*in, client_revision);

        /// For better support of old clients, that does not send ClientInfo.
        if (client_info.query_kind == ClientInfo::QueryKind::NO_QUERY)
        {
            client_info.query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
            client_info.client_name = client_name;
            client_info.client_version_major = client_version_major;
            client_info.client_version_minor = client_version_minor;
            client_info.client_revision = client_revision;
        }

        /// Set fields, that are known apriori.
        client_info.interface = ClientInfo::Interface::TCP;

        if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            /// 'Current' fields was set at receiveHello.
            client_info.initial_user = client_info.current_user;
            client_info.initial_query_id = client_info.current_query_id;
            client_info.initial_address = client_info.current_address;
        }
    }

    /// Per query settings.
    Settings & settings = query_context.getSettingsRef();
    settings.deserialize(*in);

    /// Sync timeouts on client and server during current query to avoid dangling queries on server
    /// NOTE: We use settings.send_timeout for the receive timeout and vice versa (change arguments ordering in TimeoutSetter),
    ///  because settings.send_timeout is client-side setting which has opposite meaning on the server side.
    /// NOTE: these settings are applied only for current connection (not for distributed tables' connections)
    state.timeout_setter = std::make_unique<TimeoutSetter>(socket(), settings.receive_timeout, settings.send_timeout);

    readVarUInt(stage, *in);
    state.stage = QueryProcessingStage::Enum(stage);

    readVarUInt(compression, *in);
    state.compression = static_cast<Protocol::Compression>(compression);

    readStringBinary(state.query, *in);
}


bool TCPHandlerForFirstStageExec::receiveShuffleMainTable(String & external_table_name){



    initBlockInput();

    /// The name of the temporary table for writing data, default to empty string
    //String external_table_name;
    readStringBinary(external_table_name, *in);

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"receiveShuffleMainTable : " + external_table_name );



    /// Read one block from the network and write it down
    Block block = state.block_in->read();


    if(!block){
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"receiveShuffleMainTable receive empty block");
        state.io.out->write(block);
        return false;
    }


    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," block size :" + std::to_string(block.rows()));

    {

        StoragePtr storage;
        /// If such a table does not exist, create it.
        if ( server.getGlobalShuffleTable().find(external_table_name) == server.getGlobalShuffleTable().end())
        {

            NamesAndTypesList columns = block.getNamesAndTypesList();

            String cols = "";
            for(auto e : columns){
                cols += e.name ;
                cols += ",";
            }
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"columns in block is " + cols);
            storage = StorageMemoryFifo::create(external_table_name,
                                                ColumnsDescription{columns, NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{}},100000);

            storage->startup();
            server.getGlobalShuffleTable().insert({external_table_name, storage}) ;

            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec")," create shuffle  main table : " +external_table_name);
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + " 1  use count " + std::to_string( storage.use_count()));
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + " 1  in server use count " + std::to_string(server.getGlobalShuffleTable()[external_table_name].use_count()) );
            state.io.out = storage->write(ASTPtr(), query_context.getSettingsRef());
        } else{
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"server find table  " + external_table_name);
        }
    }
    //LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + " 2  use count " + std::to_string( storage.use_count()));
    //LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + " 2  in server use count " + std::to_string(server.getGlobalShuffleTable()[external_table_name].use_count()) );


    state.io.out->write(block);
    return  true;

}


void TCPHandlerForFirstStageExec::receiveAskIfStorageBuild(String & stroage_name){


    readStringBinary(stroage_name, *in);

}

bool TCPHandlerForFirstStageExec::receiveShuffleRightTable(String & external_table_name){



    initBlockInput();


    readStringBinary(external_table_name, *in);


    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"receiveShuffleRightTable : " + external_table_name );

    /// Read one block from the network and write it down
    Block block = state.block_in->read();



    if(!block) {
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"receiveShuffleRightTable receive empty block");
        state.io.out->write(block);
        return false;
    }

    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"read block size :" + std::to_string(block.rows()));

    {
        StoragePtr storage;
        /// If such a table does not exist, create it.
        if (server.getGlobalShuffleTable().find(external_table_name) == server.getGlobalShuffleTable().end())
        {
            NamesAndTypesList columns = block.getNamesAndTypesList();

            storage = StorageMemory::create(external_table_name,
                                            ColumnsDescription{columns, NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{}});
            storage->startup();


            server.getGlobalShuffleTable().insert({external_table_name, storage}) ;
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"create shuffle  right table : " +external_table_name);
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + "  1 use count " + std::to_string( storage.use_count()));
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + "  1 in server use count " + std::to_string(server.getGlobalShuffleTable()[external_table_name].use_count()) );
            state.io.out = storage->write(ASTPtr(), query_context.getSettingsRef());
        } else{
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"), "server find table " + external_table_name);
        }

    }
    //LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + "  2 use count " + std::to_string( storage.use_count()));
    //LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),  external_table_name + "  2 in server use count " + std::to_string(server.getGlobalShuffleTable()[external_table_name].use_count()) );



    state.io.out->write(block);
    return  true;

}

bool TCPHandlerForFirstStageExec::receiveData()
{
    initBlockInput();

    /// The name of the temporary table for writing data, default to empty string
    String external_table_name;
    readStringBinary(external_table_name, *in);


    /// Read one block from the network and write it down
    Block block = state.block_in->read();

    if (block)
    {
        /// If there is an insert request, then the data should be written directly to `state.io.out`.
        /// Otherwise, we write the blocks in the temporary `external_table_name` table.
        if (!state.need_receive_data_for_insert)
        {
            StoragePtr storage;
            /// If such a table does not exist, create it.
            if (!(storage = query_context.tryGetExternalTable(external_table_name)))
            {
                NamesAndTypesList columns = block.getNamesAndTypesList();
                //storage = StorageMemoryFifo::create(external_table_name,
                //    ColumnsDescription{columns, NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{}},100000);
                storage = StorageMemory::create(external_table_name,
                                                ColumnsDescription{columns, NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{}});
                storage->startup();
                query_context.addExternalTable(external_table_name, storage);
            }
            /// The data will be written directly to the table.
            state.io.out = storage->write(ASTPtr(), query_context.getSettingsRef());
        }
        if (block)
            state.io.out->write(block);
        return true;
    }
    else{
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec") ,"receive empty block ,receive finish  ");
        return false;
    }

}


void TCPHandlerForFirstStageExec::initBlockInput()
{
    if (!state.block_in)
    {
        LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"create NativeBlockInputStream");
        if (state.compression == Protocol::Compression::Enable){
            state.maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"compression is Enable in read ");
        }
        else {
            state.maybe_compressed_in = in;
            LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"compression is Disable in read");
        }


        state.block_in = std::make_shared<NativeBlockInputStream>(
            *state.maybe_compressed_in,
            client_revision);
    }
}


void TCPHandlerForFirstStageExec::initBlockOutput(const Block & block)
{
    if (!state.block_out)
    {
        if (state.compression == Protocol::Compression::Enable)
            state.maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(
                *out, CompressionSettings(query_context.getSettingsRef()));
        else
            state.maybe_compressed_out = out;

        state.block_out = std::make_shared<NativeBlockOutputStream>(
            *state.maybe_compressed_out,
            client_revision,
            block.cloneEmpty());
    }
}


bool TCPHandlerForFirstStageExec::isQueryCancelled()
{
    if (state.is_cancelled || state.sent_all_data)
        return true;

    if (after_check_cancelled.elapsed() / 1000 < query_context.getSettingsRef().interactive_delay)
        return false;

    after_check_cancelled.restart();

    /// During request execution the only packet that can come from the client is stopping the query.
    if (static_cast<ReadBufferFromPocoSocket &>(*in).poll(0))
    {
        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::Cancel:
                if (state.empty())
                    throw NetException("Unexpected packet Cancel received from client", ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
                LOG_INFO(log, "Query was cancelled.");
                state.is_cancelled = true;
                return true;

            default:
                throw NetException("Unknown packet from client", ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
    }

    return false;
}


void TCPHandlerForFirstStageExec::sendData(const Block & block)
{
    LOG_DEBUG(&Logger::get("TCPHandlerForFirstStageExec"),"send block ,row size :" + std::to_string(block.rows()));
    initBlockOutput(block);

    writeVarUInt(Protocol::Server::Data, *out);
    writeStringBinary("", *out);

    state.block_out->write(block);
    state.maybe_compressed_out->next();
    out->next();
}


void TCPHandlerForFirstStageExec::sendException(const Exception & e)
{
    writeVarUInt(Protocol::Server::Exception, *out);
    writeException(e, *out);
    out->next();
}


void TCPHandlerForFirstStageExec::sendEndOfStream()
{
    state.sent_all_data = true;
    writeVarUInt(Protocol::Server::EndOfStream, *out);
    out->next();
}


void TCPHandlerForFirstStageExec::updateProgress(const Progress & value)
{
    state.progress.incrementPiecewiseAtomically(value);
}


void TCPHandlerForFirstStageExec::sendProgress()
{
    writeVarUInt(Protocol::Server::Progress, *out);
    auto increment = state.progress.fetchAndResetPiecewiseAtomically();
    increment.write(*out, client_revision);
    out->next();
}


void TCPHandlerForFirstStageExec::run()
{
    try
    {
        LOG_INFO(log, "start  processing connection.");
        runImpl();

        LOG_INFO(log, "Done processing connection.");
    }
    catch (Poco::Exception & e)
    {
        /// Timeout - not an error.
        if (!strcmp(e.what(), "Timeout"))
        {
            LOG_DEBUG(log, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
        }
        else
            throw;
    }
}

}
