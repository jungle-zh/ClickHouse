#include <DataStreams/RemoteBlockOutputStream.h>

#include <Client/Connection.h>
#include <common/logger_useful.h>

#include <Common/NetException.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}


RemoteBlockOutputStream::RemoteBlockOutputStream(Connection & connection_, const String & query_, const Settings * settings_,
                                                 Protocol::Client::Enum query_type_  ,const String & shuffle_table_name_ )
    : connection(connection_), query(query_), settings(settings_) ,query_type (query_type_), shuffle_table_name(shuffle_table_name_)
{
    /** Send query and receive "header", that describe table structure.
      * Header is needed to know, what structure is required for blocks to be passed to 'write' method.
      */

    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"RemoteBlockOutputStream created ,connection desc :" + connection_.getDescription());


    if(query_type == Protocol::Client::ShuffleWriteRightTable || query_type == Protocol::Client::ShuffleWriteMainTable){
        LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"query type :" + std::to_string(query_type) + " ,dont send query");
        return;
    }

    connection.sendQuery(query, "", QueryProcessingStage::Complete, settings, nullptr);

    Connection::Packet packet = connection.receivePacket();

    if (Protocol::Server::Data == packet.type)
    {
        header = packet.block;

        if (!header)
            throw Exception("Logical error: empty block received as table structure", ErrorCodes::LOGICAL_ERROR);
    }
    else if (Protocol::Server::Exception == packet.type)
    {
        packet.exception->rethrow();
        return;
    }
    else
        throw NetException("Unexpected packet from server (expected Data or Exception, got "
            + String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}


void RemoteBlockOutputStream::write(const Block & block)
{

    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream") ,"query type is " + std::to_string(query_type) +  ", write block .." + block.printColumn());
    if(query_type != Protocol::Client::ShuffleWriteRightTable && query_type != Protocol::Client::ShuffleWriteMainTable)
        assertBlocksHaveEqualStructure(block, header, "RemoteBlockOutputStream");

    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream") ,"connection desc :" + connection.getDescription() );
    try
    {
        if(query_type == Protocol::Client::Query){
            connection.sendData(block);
        } else if(query_type == Protocol::Client::ShuffleWriteMainTable){
            connection.sendShuffleMainTableData(block,shuffle_table_name);
        } else if (query_type == Protocol::Client::ShuffleWriteRightTable){
            connection.sendShuffleRightTableData(block,shuffle_table_name);
        }

    }
    catch (const NetException & e)
    {
        /// Try to get more detailed exception from server
        if (connection.poll(0))
        {
            Connection::Packet packet = connection.receivePacket();

            if (Protocol::Server::Exception == packet.type)
            {
                packet.exception->rethrow();
                return;
            }
        }

        throw;
    }
}


void RemoteBlockOutputStream::writePrepared(ReadBuffer & input, size_t size)
{
    /// We cannot use 'header'. Input must contain block with proper structure.
    connection.sendPreparedData(input, size);
}


void RemoteBlockOutputStream::writeSuffix()
{
    /// Empty block means end of data.

    //connection.sendData(Block());
    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"writeSuffix ,connection desc :" + connection.getDescription());

    if(query_type == Protocol::Client::Query){
        connection.sendData(Block());
    } else if(query_type == Protocol::Client::ShuffleWriteMainTable){
        connection.sendShuffleMainTableData(Block(),shuffle_table_name);
    } else if (query_type == Protocol::Client::ShuffleWriteRightTable){
        connection.sendShuffleRightTableData(Block(),shuffle_table_name);
    }



    /// Receive EndOfStream packet.
    Connection::Packet packet = connection.receivePacket(); //maybe block here

    if (Protocol::Server::EndOfStream == packet.type)
    {
        /// Do nothing.
        LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"receive EndOfStream in writeSuffix");
    }
    else if (Protocol::Server::Exception == packet.type)
        packet.exception->rethrow();
    else
        throw NetException("Unexpected packet from server (expected EndOfStream or Exception, got "
        + String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);


    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"connection  disconnect ,desc : " + connection.getDescription());
    connection.disconnect();  //need to disconnect ?
   // LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"connection not disconnect ");
    finished = true;
}



RemoteBlockOutputStream::~RemoteBlockOutputStream()
{
    /// If interrupted in the middle of the loop of communication with the server, then interrupt the connection,
    ///  to not leave the connection in unsynchronized state.

    LOG_DEBUG(&Logger::get("RemoteBlockOutputStream")," ~RemoteBlockOutputStream");
    if (!finished)
    {
        try
        {
            LOG_DEBUG(&Logger::get("RemoteBlockOutputStream"),"disconnect in ~RemoteBlockOutputStream");
            connection.disconnect();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
