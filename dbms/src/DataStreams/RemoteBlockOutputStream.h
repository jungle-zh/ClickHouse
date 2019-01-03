#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/Throttler.h>
#include <Core/Protocol.h>


namespace DB
{

class Connection;
class ReadBuffer;
struct Settings;


/** Allow to execute INSERT query on remote server and send data for it.
  */
class RemoteBlockOutputStream : public IBlockOutputStream
{
public:
    RemoteBlockOutputStream(Connection & connection_, const String & query_,const Settings * settings_ = nullptr, Protocol::Client::Enum query_type  = Protocol::Client::Query ,const String & shuffle_table_name = "");

    Block getHeader() const override { return header; }

    void write(const Block & block) override;
    void writeSuffix() override;
    //void writeSuffixShuffleMainTable() ;
    //void writeSuffixShuffleRightTable() ;

    /// Send pre-serialized and possibly pre-compressed block of data, that will be read from 'input'.
    void writePrepared(ReadBuffer & input, size_t size = 0);

    ~RemoteBlockOutputStream() override;

private:
    Connection & connection;
    String query;
    const Settings * settings;
    Block header;
    bool finished = false;
    Protocol::Client::Enum  query_type;

    const String  shuffle_table_name ;
};

}
