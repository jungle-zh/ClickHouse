//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>

namespace DB {


    class DataConnectionClient {


    public:

        std::shared_ptr<IBlockOutputStream> block_out; //NativeBlockOutputStream

        std::unique_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        std::shared_ptr<WriteBuffer> out;

        void sendData(Block & block);

    };


}


