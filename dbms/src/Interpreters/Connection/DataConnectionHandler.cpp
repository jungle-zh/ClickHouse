//
// Created by jungle on 19-6-17.
//

#include <Core/Block.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/DataExechangeServer.h>
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

        recievedRows = 0;
        poped_cnt = 0;
        //send_buffer.resize(100000);
        task  = server_->getTask();
        //buffer  = std::make_shared<ConcurrentBoundedQueue<Block>>();
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
           // receiveHello(); // get task id
            receivePartitionID();
            buffer = server->getBufferByPartition(father_task_partition);
            //buffer  = task->getPartitionBuffer(father_task_partition);
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

        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) && !server->isCancelled());

            /// If we need to shut down, or client disconnects.
            if (server->isCancelled() || in->eof())
                break;


            //if(!receiveBlock())  // return false  at end of data
            //   break;

            receivePackage();

            if(finished)
                break;

        }
        //setEndOfReceive(true);

    }

    /*
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
    */
    /*void DataConnectionHandler::sendCommandToClient(Protocol::DataControl::Enum  type){


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


    }*/
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
    void DataConnectionHandler::receivePackage(){

        UInt32 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type){
            case Protocol::DataControl::BLOCK_REQUEST:{
                LOG_DEBUG(log,"task " << task->getTaskId() << " receive BLOCK_REQUEST ");
                if(finished)
                    throw  Exception("dataChannel:" + dataChannel + " already finished produce data" );
                Block res ;
                buffer->pop(res); // will block if buffer is empty;  partitioned block buffer
                sendBlock(res);
                poped_cnt ++;
                LOG_DEBUG(log,"task " << task->getTaskId() << " data server poped " << poped_cnt << " block");
                if(!res){
                    finished = true;
                    LOG_DEBUG(log,"dataChannel:"+ dataChannel + " finish produce data");
                }

            }
        }



    }

    void  DataConnectionHandler::sendBlock(DB::Block &block) { // must be call in logic thread



        if (!block_out)
        {
            //if (compression == Protocol::Compression::Enable)
            //maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(*out, compression_settings);
            //else
            // maybe_compressed_out = out;
            int server_revision = 1;
            block_out = std::make_shared<NativeBlockOutputStream>(*out, server_revision, block.cloneEmpty());
        }



        writeVarUInt(Protocol::Client::Data, *out); // with one block
        //writeStringBinary(task->getTaskId(), *out);

        //size_t prev_bytes = out->count();

        block_out->write(block);
        //maybe_compressed_out->next();
        out->next();
    }

    /*
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
            receiveBlockCall(block,child_task_id);
            recievedRows += block.rows();
            return true;
        } else {
            finishCall(child_task_id);
          //  receiveBlockCall(block); //receive end block ; // todo when receive all task  block do it
            return false;
        }
    }
    */


    void DataConnectionHandler::receivePartitionID()
    {
        /// Receive `hello` packet.
        UInt32 packet_type = 0;

        readVarUInt(packet_type,*in);
        assert(packet_type == Protocol::DataControl::PARTITION_ID);
        readStringBinary(father_task_id, *in);
        readVarUInt(father_task_partition,*in);


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
