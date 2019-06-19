//
// Created by jungle on 19-6-18.
//

#pragma once

namespace DB {


    class DataConnectionClient {


    public:

        BlockOutputStreamPtr block_out; //NativeBlockOutputStream


        std::unique_ptr <Poco::Net::StreamSocket> socket;
        std::shared_ptr <ReadBuffer> in;
        std::shared_ptr <WriteBuffer> out;

    };


}


