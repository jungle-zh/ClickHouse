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
#include <Poco/Net/NetException.h>
#include <Interpreters/Task.h>
#include <Interpreters/Context.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/CompressedReadBuffer.h>
#include "DataConnectionClient.h"

namespace DB {

    DataConnectionClient::DataConnectionClient(std::string ip_, UInt16  port_,Task * task_,Context * context_ ):resolved_address(ip_,port_){

    ip = ip_;
    port = port_;
    log = &Poco::Logger::get("DataConnectionClient");
    task = task_;
    context = context_;
    timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(context->getSettings());
    //settings = settings_;
    //compression_settings =  CompressionSettings(settings) ;
    }
    void  DataConnectionClient::sendPartitionId(std::string taskId, int partionId) {


        writeVarUInt(Protocol::DataControl::PARTITION_ID, *out);
        writeStringBinary(taskId,*out);
        writeVarUInt(partionId,*out);


        out->next();

    }

    void DataConnectionClient::receiveHello()
    {
        //LOG_TRACE(log_wrapper.get(), "Receiving hello");


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

        while (!connected){

        try {
            auto timeout = Poco::Timespan(50000);
            LOG_DEBUG(log,"task " + task->getTaskId() + " sender  start to  connect to remote ,time_out:" << timeouts.connection_timeout.microseconds() );
            socket->connect(resolved_address, timeout);
            connected = true;
        }catch (Poco::Net::ConnectionAbortedException& exception){
            connected = false;
            LOG_ERROR(log,exception.what());

        }catch (Poco::Net::ConnectionResetException& exception){
            connected = false;
            LOG_ERROR(log,exception.what());
        }
        catch (Poco::Net::ConnectionRefusedException& exception){
            connected = false;
            LOG_ERROR(log,exception.what());
        }
        if(!connected){
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            LOG_WARNING(log,"connect failed, retry.. ");
        }

        }

        socket->setReceiveTimeout(timeouts.receive_timeout);
        socket->setSendTimeout(timeouts.send_timeout);
        socket->setNoDelay(true);


        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

        connected = true;
        return connected;
    }
    Block DataConnectionClient::read(){

        UInt32 packet_type = Protocol::DataControl::BLOCK_REQUEST;
        writeVarUInt(packet_type,*out);

        return  receiveBlock();

    }
    Block DataConnectionClient::receiveBlock() {

        if (!block_in)
        {
            int client_revision = 3;
            if (compression == Protocol::Compression::Enable)
                maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
            else
                maybe_compressed_in = in;

            block_in = std::make_shared<NativeBlockInputStream>(
                    *maybe_compressed_in,
                    client_revision);
        }

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);
        assert(packet_type == Protocol::Client::Data );

        //String child_task_id;
        //readStringBinary(child_task_id, *in);

        //LOG_DEBUG(log,"current task:" + server->getTask()->getTaskId() + " receive child task： " + child_task_id + " data");
        /// Read one block from the network and write it down
        Block block = block_in->read();

        if(!block){
            LOG_DEBUG(log,"current task:" + task->getTaskId()  + " read all data");
            finished = true;
        }
        return  block;
    }
    /***
    void  DataConnectionClient::sendBlock(DB::Block &block) { // must be call in logic thread



        if (!block_out)
        {
            //if (compression == Protocol::Compression::Enable)
                //maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(*out, compression_settings);
            //else
               // maybe_compressed_out = out;
            server_revision = 1;
            block_out = std::make_shared<NativeBlockOutputStream>(*out, server_revision, block.cloneEmpty());
        }

        while(stopSend) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }


        writeVarUInt(Protocol::Client::Data, *out); // with one block
        writeStringBinary(task->getTaskId(), *out);

        block_out->write(block);
        //maybe_compressed_out->next();
        out->next();
    }

    bool DataConnectionClient::receiveStopCommand(){


        UInt64 packet_type = 0;

        readVarUInt(packet_type, *in);

        if (packet_type == Protocol::DataControl::STOP){

            LOG_INFO(log,"DataConnectionClient receive StopSendCommand  "  );
            return true;
        } else if(packet_type == Protocol::DataControl::START){

            LOG_INFO(log,"DataConnectionClient receive StartSendCommand  "  );
            return false;
        } else {
            LOG_ERROR(log,"DataConnectionClient receive unknow command :"  << packet_type);
            throw  Exception();
        }

    }
    void DataConnectionClient::startListen() {
        pool.schedule(std::bind(&DataConnectionClient::listen, this));
    }
    void DataConnectionClient::listen() {

        while (true) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    settings.poll_interval * 1000000) );


            if (in->eof())
                break;

            bool  cmd  = receiveStopCommand();
            if(cmd){  // command from upstream
                stopSend = true;
                //writeStringBinary("client_ack", *out);

            } else {
                stopSend = false;
                //writeStringBinary("client_ack", *out);
            }
            //out->next();

        }
    }
    ***/

}
