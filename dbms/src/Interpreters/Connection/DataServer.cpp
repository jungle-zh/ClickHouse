//
// Created by jungle on 19-6-17.
//

#include "DataServer.h"
#include <Interpreters/DataReceiver.h>

namespace DB {


    void DataServer::fill(Block & block) {
        receiver_->fill(block);
    }

}