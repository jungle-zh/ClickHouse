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

namespace DB {


    void TaskConnectionClient::init() {

        in = std::make_shared<ReadBufferFromPocoSocket>(*socket);
        out = std::make_shared<WriteBufferFromPocoSocket>(*socket);
        out_stream = std::make_shared<TaskOutputStream>(out,version);


    }
    void TaskConnectionClient::sendTask(Task & task){



        writeVarUInt(Protocol::TaskClient::TaskReq, *out);

        out_stream.write(task);


    }


    DataReceiverInfo TaskConnectionClient::applyResource(std::string taskId){


        writeVarUInt(Protocol::TaskClient::AppalyResource, *out);
        writeStringBinary(taskId, *out);

        UInt32 port = 0;
        std::string ip ;
        readVarUInt(port, *in);
        readStringBinary(ip,*in);
        return DataReceiverInfo(ip,port);


    }
}
