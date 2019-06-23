//
// Created by jungle on 19-6-17.
//

#include <Core/Block.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include "DataConnectionHandler.h"

namespace DB {

    void DataConnectionHandler::runImpl() {

        connection_context = server->context();
        connection_context.setSessionContext(connection_context);

        Settings global_settings = connection_context.getSettings();

        socket().setReceiveTimeout(global_settings.receive_timeout);
        socket().setSendTimeout(global_settings.send_timeout);
        socket().setNoDelay(true);

        connection_context.setProgressCallback([this](const Progress &value) { return this->updateProgress(value); });


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


        sendHello();

        while (1) {
            /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
            while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(
                    global_settings.poll_interval * 1000000) && !server->isCancelled());

            /// If we need to shut down, or client disconnects.
            if (server->isCancelled() || in->eof())
                break;


            if(!receiveData())  // return false  at end of data
                break;


        }

    }

    bool DataConnectionHandler::receiveData() {

        Block block = in->read(); //NativeBlockInputStream read and deserialize

        if (block){
            server->fill(block,senderId);
            return true;
        } else {
            return false;
        }
    }
}
