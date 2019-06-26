//
// Created by jungle on 19-6-17.
//

#include "DataServer.h"
#include <Interpreters/DataReceiver.h>

namespace DB {


    void DataServer::fill(Block & block,std::string senderId) {
        receiver_->fill(block,senderId);
    }

    bool DataServer::getStartToReceive() {
        return receiver_->getStartToReceive();
    }

}