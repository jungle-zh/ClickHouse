//
// Created by jungle on 19-6-17.
//

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include "TaskConnectionHandler.h"


namespace DB {


    void TaskConnectionHandler::runImpl() {

        connection_context = server.context();
        connection_context.setSessionContext(connection_context);

        Settings global_settings = connection_context.getSettings();

        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        in = std::make_shared<TaskInputStream>(std::make_shared<ReadBufferFromPocoSocket>(socket()),version);
        out = std::make_shared<WriteBufferFromPocoSocket>(socket());


        connection_context.setProgressCallback([this](const Progress &value) { return this->updateProgress(value); });


        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) && !server.isCancelled());

            /// If we need to shut down, or client disconnects.
            if (server.isCancelled() || in->eof())
                break;


            if(!receiveTask())  // return false  at end of data
                break;

        }

        initTask();
        execTask(); // after execute ,all data is send to task dest ,
        finishTask();

        // out->write()  task finish flag
    }

    bool TaskConnectionHandler::receiveTask() {

        task = in->read(); // read and deserialize , include execNode info and task source and dest info
        if (task){
            return true;
        } else {
            return false;
        }
    }

    void TaskConnectionHandler::initTask(){

        task->init(); // create sender and receiver for task
    }
    void TaskConnectionHandler::execTask() {

        task->execute();
    }

    void TaskConnectionHandler::finishTask() {

        task->finish();
    }



}
