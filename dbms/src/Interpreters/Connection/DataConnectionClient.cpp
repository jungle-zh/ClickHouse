//
// Created by jungle on 19-6-18.
//

#include <common/logger_useful.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Core/Protocol.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Common/ClickHouseRevision.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include "DataConnectionClient.h"

namespace DB {

    void  DataConnectionClient::sendHello(std::string taskId, int partionId) {


        writeVarUInt(Protocol::Client::Hello, *out);
        writeStringBinary((DBMS_NAME " ") ,*out);
        writeVarUInt(DBMS_VERSION_MAJOR, *out);
        writeVarUInt(DBMS_VERSION_MINOR, *out);
        writeVarUInt(ClickHouseRevision::get(), *out);

        writeStringBinary(taskId,*out);
        writeVarUInt(partionId,*out);


        out->next();

    }


    void DataConnectionClient::disconnect()
    {
        //LOG_TRACE(log_wrapper.get(), "Disconnecting");

        in = nullptr;
        out = nullptr; // can write to socket
        if (socket)
            socket->close();
        socket = nullptr;
        connected = false;
    }


    bool  DataConnectionClient::connect(){

        if (connected)
            disconnect();

        socket = std::make_unique<Poco::Net::StreamSocket>();

        try {

            socket->connect(resolved_address, timeouts.connection_timeout);
            socket->setReceiveTimeout(timeouts.receive_timeout);
            socket->setSendTimeout(timeouts.send_timeout);
            socket->setNoDelay(true);

        }catch (Exception e){
            LOG_ERROR(log,e.what());
            connected = false;
        }

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;
        return connected;
    }
    void  DataConnectionClient::sendBlock(DB::Block &block) { // must be call in logic thread



        if (!block_out)
        {
            if (compression == Protocol::Compression::Enable)
                maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(*out, compression_settings);
            else
                maybe_compressed_out = out;

            block_out = std::make_shared<NativeBlockOutputStream>(*maybe_compressed_out, server_revision, block.cloneEmpty());
        }

        if(!stopSend){


            writeVarUInt(Protocol::Client::Data, *out); // with one block
            writeStringBinary("dataClient", *out);

            //size_t prev_bytes = out->count();

            block_out->write(block);
            maybe_compressed_out->next();
            out->next();


            
        } else {
            //wait until notified in listen
            while(stopSend) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    bool DataConnectionClient::receiveStopCommand(){


        UInt64 packet_type = 0;

        readVarUInt(packet_type, *in);

        if (packet_type == Protocol::Server::StopSendCommand){

            LOG_INFO(log,"DataConnectionClient receive StopSendCommand  "  );
            return true;
        } else if(packet_type == Protocol::Server::StartSendCommand){

            LOG_INFO(log,"DataConnectionClient receive StartSendCommand  "  );
            return false;
        } else {
            LOG_ERROR(log,"DataConnectionClient receive unknow command :"  << packet_type);
            throw  Exception();
        }

    }
    void DataConnectionClient::listen() { // io thread

        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) );

            /// If we need to shut down, or client disconnects.
           // if (server->isCancelled() || in->eof())
           //     break;


            if(receiveStopCommand()){  // command from upstream
                stopSend = true;
                writeVarUInt(Protocol::DataControl::CLIENT_ACK, *out);

            } else {
                stopSend = false;
                writeVarUInt(Protocol::DataControl::CLIENT_ACK, *out);
            }

        }
    }


}
