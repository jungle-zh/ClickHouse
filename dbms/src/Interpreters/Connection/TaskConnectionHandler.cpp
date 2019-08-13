//
// Created by jungle on 19-6-17.
//

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Core/Protocol.h>
#include <Interpreters/Context.h>
#include "TaskConnectionHandler.h"


namespace DB {

    void TaskConnectionHandler::run()
    {
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


    void TaskConnectionHandler::runImpl() {

        connection_context = server->context();
        connection_context->setSessionContext(*connection_context);

        Settings global_settings = connection_context->getSettings();

        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        in = std::make_shared<ReadBufferFromPocoSocket>(socket());
        in_stream = std::make_shared<TaskInputStream>(in,1,connection_context);
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());


        //connection_context->setProgressCallback([this](const Progress &value) { return this->updateProgress(value); });


        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) && !server->isCancelled());

            /// If we need to shut down, or client disconnects.
            if (server->isCancelled() || in->eof())
                break;


            receivePackage();
        }


    }

    void TaskConnectionHandler::receivePackage() {

        //first receive apply resource req , then task  req;

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type) {

            case Protocol::TaskClient::AppalyResource:{
                std::string taskTmpId ;
                readStringBinary(taskTmpId,*in);
                taskId = taskTmpId;
                receiveApplyRequest();
                break;
            }

            case Protocol::TaskClient::TaskReq: {
                readStringBinary(taskType,*in);
                std::string taskTmpId ;
                readStringBinary(taskTmpId,*in);
                if(taskType == "midTask"){
                    assert(taskId == taskTmpId);
                } else if(taskType == "scanTask") {
                    taskId = taskTmpId;
                }
                receiveTask();
                break;
            }

            default:
                throw Exception("Unknown packet " + toString(packet_type));
        }


    }

    void TaskConnectionHandler::receiveApplyRequest() {


        DataReceiverInfo resource =  server->applyResource() ; // need to be thread safe ,apply ip and host  for task dataReceiver

        writeVarUInt(resource.dataPort, *out);
        writeStringBinary(resource.ip, *out);
        out->next();

    }

    void TaskConnectionHandler::receiveTask() {


        task = in_stream->read(); // read and deserialize , include execNode info and task source and dest info
        if (task) {
            //task->setDataConnectionHandlerFactory(server->getDataConnectionHandlerFactory());
            pool.schedule(std::bind(&TaskConnectionHandler::runTask, this));

        }

    }

    void TaskConnectionHandler::runTask(){

        try {

            task->init();
            task->execute();

        }catch (...){
            exception = std::current_exception();
        }

    }


    void TaskConnectionHandler::receiveTaskDone(){ // taskSceduler send task done and finish the handler

    }

    void TaskConnectionHandler::receiveCheckTask(){

    }




}
