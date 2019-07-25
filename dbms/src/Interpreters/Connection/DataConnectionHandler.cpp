//
// Created by jungle on 19-6-17.
//

#include <Core/Block.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/DataReceiver.h>
#include <common/logger_useful.h>
#include <IO/VarInt.h>
#include <Core/Protocol.h>
#include <Common/NetException.h>
#include <IO/CompressedReadBuffer.h>
#include "DataConnectionHandler.h"

namespace DB {

    namespace ErrorCodes
    {
        extern const int DATA_CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
        extern const int DATA_UNKNOWN_DATABASE;
        extern const int DATA_UNKNOWN_EXCEPTION;
        extern const int DATA_UNKNOWN_PACKET_FROM_CLIENT;
        extern const int DATA_POCO_EXCEPTION;
        extern const int DATA_STD_EXCEPTION;
        extern const int DATA_SOCKET_TIMEOUT;
        extern const int DATA_UNEXPECTED_PACKET_FROM_CLIENT;
    }


    void DataConnectionHandler::runImpl() {

        connection_context = &server->context();
        connection_context->setSessionContext(*connection_context);

        Settings global_settings = connection_context->getSettings();

        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        connection_context->setProgressCallback([this](const Progress &value) { return this->updateProgress(value); });


        in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());

        if (in->eof())
        {
            LOG_WARNING(log, "Client has not sent any data.");
            return;
        }

        try
        {
            receiveHello(); // get task id
        }
        catch (const Exception & e) /// Typical for an incorrect username, password, or address.
        {
            if (e.code() == ErrorCodes::DATA_CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
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


        //sendHello();


        while(1){

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if(startToReceive){

                //out send start signal to data connection client
                // receiveStart();
                sendCommandToClient(Protocol::DataControl::START);
                break;
            }
        }




        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) && !server->isCancelled());

            /// If we need to shut down, or client disconnects.
            if (server->isCancelled() || in->eof())
                break;



            if(highWaterMarkCall()){
                sendCommandToClient(Protocol::DataControl::STOP);
            } else {
                sendCommandToClient(Protocol::DataControl::START);
            }

            if(!receiveBlock())  // return false  at end of data
                break;



        }
        setEndOfReceive(true);

    }


    void DataConnectionHandler::sendCommandToClient(Protocol::DataControl::Enum  type){


        UInt32  rep = 0 ;
        writeVarUInt(type, *out);
        readVarUInt(rep,*in);

    }

    void DataConnectionHandler::initBlockInput()
    {
        if (!block_in)
        {
            if (compression == Protocol::Compression::Enable)
                maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
            else
                maybe_compressed_in = in;

            block_in = std::make_shared<NativeBlockInputStream>(
                    *maybe_compressed_in,
                    client_revision);
        }
    }
    bool DataConnectionHandler::receiveBlock() {

        initBlockInput();

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);
        assert(packet_type == Protocol::Client::Data );

        String external_table_name;
        readStringBinary(external_table_name, *in);

        /// Read one block from the network and write it down
        Block block = block_in->read();


        if (block){
            //server->fill(block,senderId);
            receiveBlockCall(block);
            return true;
        } else {
            return false;
        }
    }

    bool DataConnectionHandler::getEndOfReceive() {
        return endOfReceive;
    }


    void DataConnectionHandler::receiveHello()
    {
        /// Receive `hello` packet.
        UInt64 packet_type = 0;


        readVarUInt(packet_type, *in);

        if (packet_type != Protocol::Client::Hello)
        {
            /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
              * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
              */
            if (packet_type == 'G' || packet_type == 'P')
            {
                writeString("HTTP/1.0 400 Bad Request\r\n\r\n"
                            "Port " + server->config().getString("tcp_port") + " is for clickhouse-client program.\r\n"
                                                                              "You must use port " + server->config().getString("http_port") + " for HTTP.\r\n",
                            *out);

                throw Exception("Client has connected to wrong port", ErrorCodes::DATA_CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
            }
            else
                throw NetException("Unexpected packet from client", ErrorCodes::DATA_UNEXPECTED_PACKET_FROM_CLIENT);
        }

        readStringBinary(client_name, *in);
        readVarUInt(client_version_major, *in);
        readVarUInt(client_version_minor, *in);
        readVarUInt(client_revision, *in);
        readStringBinary(upstream_task_id,*in);
        readVarUInt(upstream_task_partition,*in);


        LOG_DEBUG(&Logger::get("DataConnectionHandler"), "Connected " << client_name
                                    << " version " << client_version_major
                                    << "." << client_version_minor
                                    << "." << client_revision
                                    << " task id " << upstream_task_id
                                    << " partition " << upstream_task_partition
                                    << ".");

    }


    void DataConnectionHandler::sendException(const Exception & e)
    {
        writeVarUInt(Protocol::Server::Exception, *out);
        writeException(e, *out);
        out->next();
    }


    void DataConnectionHandler::sendEndOfStream()
    {
        state.sent_all_data = true;
        writeVarUInt(Protocol::Server::EndOfStream, *out);
        out->next();
    }


}
