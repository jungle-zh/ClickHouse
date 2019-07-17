//
// Created by usser on 2019/6/17.
//

#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include <Interpreters/Connection/DataConnectionHandler.h>
#include "DataReceiver.h"

namespace DB {

 void DataReceiver::init() {

     while(connections.size() < childTaskIds.size()){
         std::this_thread::sleep_for(std::chrono::milliseconds(100));// wait for all child to connect
     }


     auto receiveHashTable  = [this](Block & b) {
         task->receiveHashTable(b);
     };

     auto receiveMainTable = [this](Block & b){
         //task->receiveMainTable(b);
         task->receiveBlock(b);
     };
     auto highWaterMark  = [this]() ->  bool {
         return task->highWaterMark();
     };


     if(exechangeType == DataExechangeType::toneshufflejoin
      ||exechangeType == DataExechangeType::tone2onejoin
      ||exechangeType == DataExechangeType::ttwoshufflejoin){

         for(auto p : connections){

             if(beloneTo(p.first,rightTableStageId)){
                 p.second->setStartToReceive(true);
                 p.second->receiveBlockCall  = receiveHashTable;
             }
         }

         for(auto p : connections){  // wait all right table read done
             if(beloneTo(p.first,rightTableStageId)){
                 while(!p.second->getEndOfReceive()){ // hash table preprare
                     std::this_thread::sleep_for(std::chrono::milliseconds(100));
                 }
             }
         }

     }



     for(auto p : connections){

         p.second->highWaterMarkCall = highWaterMark;
         if(beloneTo(p.first,mainTableStageId)){   // start to receive main table
             p.second->setStartToReceive(true);
             p.second->receiveBlockCall  = receiveMainTable;
         }
     }



 }

 void DataReceiver::startToAccept() {
     server = std::make_unique<DataServer>(port,buffer);
     server->start(); // start receive data connection and create handler
 }


}