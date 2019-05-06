//
// Created by Administrator on 2019/5/4.
//


namespace DB {

void ExchangeNodeTcpHandler::runImpl(){

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    if (in->eof()){
        LOG_WARNING(log, "Client has not sent any data.");
        return;
    }

    try{
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


    receiveHeader(); // join , agg , sort  ,table name

    while (1) {
        /// We are waiting for a packet from the client. Thus, every `POLL_INTERVAL` seconds check whether we need to shut down.
        while (!static_cast<ReadBufferFromPocoSocket &>(*in).poll(global_settings.poll_interval * 1000000) &&
               !server.isCancelled());

        /// If we need to shut down, or client disconnects.
        if (node.isCancelled() || in->eof())
            break;

        receiveData();

    }

}

bool ExchangeNodeTcpHandler::receiveHeader(){
    initBlockInput();


    readVarUInt(packageType, *in);

    readStringBinary(tableName, *in);

}

Block ExchangeNodeTcpHandler::read(){

    return  blockLocalContainer.read();
}
bool ExchangeNodeTcpHandler::receiveData() {
    initBlockInput();

    Block block = state.block_in->read();

    if (block){

        blockLocalContainer.write(block);
        return true;
    } else {
        return false;
    }
}
void ExchangeNodeTcpHandler::initBlockInput()
{
    if (!state.block_in)
    {
        if (state.compression == Protocol::Compression::Enable)
            state.maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in);
        else
            state.maybe_compressed_in = in;

        state.block_in = std::make_shared<NativeBlockInputStream>(
                *state.maybe_compressed_in,
                client_revision);
    }
}

void ExchangeNodeTcpHandler::run(){

    try
    {
        runImpl();

        LOG_INFO(log, "Done processing connection.");
    }
    catch (Poco::Exception & e)
    {
        /// Timeout - not an error.
        if (!strcmp(e.what(), "Timeout"))
        {
            LOG_DEBUG(log, "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                                                     << ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what());
        }
        else
            throw;
    }



}



}
