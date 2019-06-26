//
// Created by usser on 2019/6/17.
//

#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include "DataReceiver.h"

namespace DB {

 bool DataReceiver::getStartToReceive() {
     return  startToReceive;
 }
 void DataReceiver::setStartToReceive(bool startToReceive_) {
     startToReceive = startToReceive_;
 }
 void DataReceiver::startToAccept() {
     server = std::make_unique<DataServer>(port,buffer);
     server->start(); // start receive data connection and create handler
 }


}