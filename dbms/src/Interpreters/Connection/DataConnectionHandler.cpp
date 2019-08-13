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
#include <Server/IServer.h>
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


    DataConnectionHandler::DataConnectionHandler(const Poco::Net::StreamSocket & socket_,DataServer * server_)
            :Poco::Net::TCPServerConnection(socket_),
             server(server_), connection_context(server_->context()) {

        log = &Poco::Logger::get("DataConnectionHandler");


        //block_out = std::make_shared<NativeBlockOutputStream>(std::make_shared<WriteBufferFromPocoSocket>(socket()),1);

    };


    void DataConnectionHandler::runImpl() {

        connection_context = server->context();
        connection_context->setSessionContext(*connection_context);

        Settings global_settings = connection_context->getSettings();

        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        //connection_context->setProgressCallback([this](const Progress &value) { return this->updateProgress(value); });


        in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());

        UInt64 version = 1;
        block_in = std::make_shared<NativeBlockInputStream>(*in,version);

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

        server->addConnection(this);
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


            if(!receiveBlock())  // return false  at end of data
                break;



        }
        setEndOfReceive(true);

    }


    void DataConnectionHandler::checkHighWaterMark(){ // only write
        auto cmd = [&](void){
            if(highWaterMarkCall()){
                sendCommandToClient(Protocol::DataControl::STOP);
            } else {
                sendCommandToClient(Protocol::DataControl::START);
            }
        };
        pool.schedule(cmd);

    }

    void DataConnectionHandler::sendCommandToClient(Protocol::DataControl::Enum  type){


        std::string  rep ;
        writeVarUInt(type, *out);
        out->next();
        //readStringBinary(rep,*in);
        //assert(rep == "client_ack");

        std::string cmd ;
        if(type == Protocol::DataControl::START){
            cmd = "start";
        } else if(type == Protocol::DataControl::STOP){
            cmd = "stop";
        }
      ;
        LOG_DEBUG(log, "task:" + server->getTask()->getTaskId() + " send " + cmd + " to child task: " +  child_task_id + " success" );


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

        String child_task_id;
        readStringBinary(child_task_id, *in);

        LOG_DEBUG(log,"current task:" + server->getTask()->getTaskId() + " receive child task： " + child_task_id + " data");
        /// Read one block from the network and write it down
        Block block = block_in->read();


        if (block){
            //server->fill(block,senderId);
            receiveBlockCall(block);
            return true;
        } else {
            finishCall(child_task_id);
          //  receiveBlockCall(block); //receive end block ; // todo when receive all task  block do it
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
               // writeString("HTTP/1.0 400 Bad Request\r\n\r\n"
               //             "Port " + server->config().getString("tcp_port") + " is for clickhouse-client program.\r\n"
               //                                                               "You must use port " + server->config().getString("http_port") + " for HTTP.\r\n",
               //             *out);

                throw Exception("Client has connected to wrong port", ErrorCodes::DATA_CLIENT_HAS_CONNECTED_TO_WRONG_PORT);
            }
            else
                throw NetException("Unexpected packet from client", ErrorCodes::DATA_UNEXPECTED_PACKET_FROM_CLIENT);
        }

        readStringBinary(client_name, *in);
        readVarUInt(client_version_major, *in);
        readVarUInt(client_version_minor, *in);
        readVarUInt(client_revision, *in);
        readStringBinary(child_task_id,*in);
        readVarUInt(upstream_task_partition,*in);


        LOG_DEBUG(&Logger::get("DataConnectionHandler"), "Connected " << client_name
                                    << " version " << client_version_major
                                    << "." << client_version_minor
                                    << "." << client_revision
                                    << " task id " << child_task_id
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
        //state.sent_all_data = true;
        writeVarUInt(Protocol::Server::EndOfStream, *out);
        out->next();
    }
    void DataConnectionHandler::run(){

        try
        {
            runImpl();

            //LOG_INFO(log, "Done processing connection.");
        }
        catch (Poco::Exception & e)
        {
            /// Timeout - not an error.
            if (!strcmp(e.what(), "Timeout"))
            {
                //LOG_DEBUG(log, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                //                                         << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
            }
            else
                throw;
        }

    }


}
