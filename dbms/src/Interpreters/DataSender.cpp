//
// Created by usser on 2019/6/17.
//

#include <Interpreters/DataSender.h>

namespace DB {

    void DataSender::tryConnect(){

        for(auto e :    dest.receiverInfo){
           auto connClient =  std::make_shared<DataConnectionClient>(e.second.ip,e.second.dataPort);
            if(!connClient->connect()){
                LOG_ERROR(&Logger::get("DataSender"),"connect failed ");
            }
            connClient->sendHello(task->getTaskId(),e.first); // send taskId and partition,receive send command in listen
            connections.insert({e.first,connClient});
        }

    }

    void DataSender::send(DB::Block &block) {

        // shuffle block by distributekey and send to all executors

        std::map<UInt32,Block> toSend =  repartitionByKey(block);

        for(auto partion2block : toSend){
            connections[partion2block.first]->sendBlock(partion2block.second);
        }
    }

    std::map<UInt32,Block> DataSender::repartitionByKey(Block & block){

        (void)block;
        std::map<UInt32,Block> blocks ; // partition id - > block

        return blocks;

    }
}

