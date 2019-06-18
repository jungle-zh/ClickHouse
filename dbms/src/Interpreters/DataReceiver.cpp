//
// Created by usser on 2019/6/17.
//

#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include "DataReceiver.h"

namespace DB {

 void DataReceiver::startToReceive() {
     server = std::make_unique<DataServer>(8000,buffer);
     server->start(); // start receive data connection and create handler
 }


}