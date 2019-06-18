//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Core/Block.h>
#include <Interpreters/Task.h>
#include <Interpreters/Connection/DataServer.h>

namespace DB {



class DataReceiver {

public:
    DataReceiver(DataSource source, Blocks & inputHeader);
    void startToReceive(); // receive and deserialize data
    Block read();             // call by logic thread
    void fill(Block & block); // call by io thread

    std::unique_ptr<DataServer>  server;
    std::shared_ptr<DataBuffer>  buffer; // read and fill in different thread ,buffer need to be thread safe

};


}

