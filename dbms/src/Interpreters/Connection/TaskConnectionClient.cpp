//
// Created by usser on 2019/6/19.
//

#include "TaskConnectionClient.h"
#include <Interpreters/Task.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Core/Protocol.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/NetException.h>

namespace DB {


    void TaskConnectionClient::connect()
    {
        try
        {
            if (connected)
                disconnect();

           // LOG_TRACE(log_wrapper.get(), "Connecting. Database: " << (default_database.empty() ? "(not specified)" : default_database) << ". User: " << user
           //                                                       << (static_cast<bool>(secure) ? ". Secure" : "") << (static_cast<bool>(compression) ? "" : ". Uncompressed") );

            socket = std::make_unique<Poco::Net::StreamSocket>();
            socket->connect(resolved_address, timeouts.connection_timeout);
            socket->setReceiveTimeout(timeouts.receive_timeout);
            socket->setSendTimeout(timeouts.send_timeout);
            socket->setNoDelay(true);

            in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
            out = std::make_shared<WriteBufferFromPocoSocket>(*socket);
            out_stream = std::make_shared<TaskOutputStream>(out,1);
            connected = true;

            sendHello();
            receiveHello();

           /* LOG_TRACE(log_wrapper.get(), "Connected to " << server_name
                                                         << " server version " << server_version_major
                                                         << "." << server_version_minor
                                                         << "." << server_revision
                                                         << ".");
                                                         */
        }
        catch (Poco::Net::NetException & e)
        {
            disconnect();

            /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
            //throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::NETWORK_ERROR);
        }
        catch (Poco::TimeoutException & e)
        {
            disconnect();

            /// Add server address to exception. Also Exception will remember stack trace. It's a pity that more precise exception type is lost.
            //throw NetException(e.displayText(), "(" + getDescription() + ")", ErrorCodes::SOCKET_TIMEOUT);
        }
    }

    void TaskConnectionClient::sendTask(Task & task){

        writeVarUInt(Protocol::TaskClient::TaskReq, *out);
        if(task.hasScan){
            writeStringBinary("scanTask",*out);
        } else if(task.isResult){
            writeStringBinary("resultTask",*out);
        } else {
            writeStringBinary("midTask",*out);
        }

        writeStringBinary(task.getTaskId(), *out);
        out_stream->write(task);

    }


    /*
    DataReceiverInfo TaskConnectionClient::applyResource(std::string taskId){


        writeVarUInt(Protocol::TaskClient::AppalyResource, *out);
        writeStringBinary(taskId, *out);

        out->next(); // flush buffer

        UInt32 port = 0;
        std::string ip ;
        readVarUInt(port, *in);
        readStringBinary(ip,*in);
        DataReceiverInfo info ;
        info.ip = ip;
        info.dataPort = port;
        return info;


    }
     */
    std::string  TaskConnectionClient::askReady(){
        writeVarUInt(Protocol::TaskClient::IsDataExechangeSourceReady, *out);
        out->next(); // flush buffer
        std::string isReady ;
        readStringBinary(isReady,*in);
        return  isReady;
    }
    TaskSource TaskConnectionClient::getExechangeSourceInfo(std::string taskId){

        writeVarUInt(Protocol::TaskClient::DataExechangeSource, *out);
        writeStringBinary(taskId, *out);

        out->next(); // flush buffer

        UInt32 port = 0;
        std::string ip ;
        readVarUInt(port, *in);
        readStringBinary(ip,*in);
        TaskSource info ;
        info.taskId = taskId;
        info.ip = ip;
        info.dataPort = port;
        return info;

    }
}
