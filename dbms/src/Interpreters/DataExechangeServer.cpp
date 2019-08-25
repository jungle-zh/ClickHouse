//
// Created by usser on 2019/6/17.
//

#include <Interpreters/Connection/DataConnectionHandlerFactory.h>
#include <Interpreters/Connection/DataConnectionHandler.h>
#include "DataExechangeServer.h"

namespace DB {

 void DataExechangeServer::init() {


     /*
     while(server->connections().size() < childTaskIds.size()){
         std::this_thread::sleep_for(std::chrono::milliseconds(100));// wait for all child to connect
     }


     auto receiveHashTable  = [this](Block & b,std::string childTaskId) {
         task->receiveHashTable(b,childTaskId);
     };

     auto receiveMainTable = [this](Block & b,std::string childTaskId){
         //task->receiveMainTable(b);
         (void) childTaskId;
         task->receiveBlock(b);
     };


     auto highWaterMark  = [this]() ->  bool {
         return task->highWaterMark();
     };
     auto childTaskFinish = [this] (std::string childTaskId) {
         task->addFinishChildTask(childTaskId);
         if(task->allChildTaskFinish()){
             Block last;
             LOG_DEBUG(log,"task :" + task->getTaskId() + " child all finished");
             task->receiveBlock(last);
         }
     };


     if(exechangeType == DataExechangeType::toneshufflejoin
      ||exechangeType == DataExechangeType::tone2onejoin
      ||exechangeType == DataExechangeType::ttwoshufflejoin){

         for(auto p : server->connections()){

             if(beloneTo(p.first,rightTableStageId)){
                 p.second->setStartToReceive(true);
                 p.second->receiveBlockCall  = receiveHashTable;
                 p.second->finishCall = childTaskFinish;
             }
         }

         for(auto p : server->connections()){  // wait all right table read done
             if(beloneTo(p.first,rightTableStageId)){
                 while(!p.second->getEndOfReceive()){ // hash table preprare
                     std::this_thread::sleep_for(std::chrono::milliseconds(100));
                 }
                 if(p.second->getEndOfReceive()){
                     LOG_DEBUG(log,"task:" << task->getTaskId()
                     << " received all hash table data from stage :" << rightTableStageId
                     << " ,total rows:" << p.second->recievedRows);
                 }
             }
         }
     }


     for(auto p : server->connections()){

         p.second->highWaterMarkCall = highWaterMark;
         if(beloneTo(p.first,mainTableStageIds)){   // start to receive main table
             p.second->setStartToReceive(true);
             p.second->receiveBlockCall  = receiveMainTable;
             p.second->finishCall = childTaskFinish;
         }
     }

    */

 }

 void DataExechangeServer::startToAccept() {
     server = std::make_shared<DataServer>(partitionBuffer,port,context,task);
     server->start(); // start receive data connection and create handler
     LOG_DEBUG(log," task " +  task->getTaskId() + " receiver start to accept on port " << port);
 }

 bool DataExechangeServer::beloneTo(const std::string taskId, std::string stageId) {

     auto stringVec = split(taskId,"_");
     std::string res ;
     assert(stringVec.size() == 3);
     res = stringVec[0] + "_" + stringVec[1];
     if(res == stageId)
         return true;
     else
         return false;

 }

 bool DataExechangeServer::beloneTo(const std::string taskId, std::vector<std::string> stageIds) {

     for(auto s : stageIds){
         if(beloneTo(taskId,s))
             return true;
     }
     return false;
 }
   std::vector<std::string> DataExechangeServer::split(const std::string& str, const std::string& delim) {
        std::vector<std::string>  res;
        if("" == str) return res;
        //先将要切割的字符串从string类型转换为char*类型
        char * strs = new char[str.length() + 1] ; //不要忘了
        strcpy(strs, str.c_str());

        char * d = new char[delim.length() + 1];
        strcpy(d, delim.c_str());

        char *p = strtok(strs, d);
        while(p) {
            std::string s = p; //分割得到的字符串转换为string类型
            res.push_back(s); //存入结果数组
            p = strtok(NULL, d);
        }

        return res;
    }


}