//
// Created by usser on 2019/6/17.
//

#include <Interpreters/DataSender.h>

namespace DB {

    void DataSender::tryConnect(){

        for(auto e :    dest.receiverInfo){
           auto connClient =  std::make_shared<DataConnectionClient>(e.second.ip,e.second.dataPort,task);
            if(!connClient->connect()){
                LOG_ERROR(log,"connect failed ");
            } else {
                LOG_DEBUG(log,"task " + task->getTaskId() + " sender  connect to server :" << e.second.ip );
            }
            connClient->sendHello(task->getTaskId(),e.first); // send taskId and partition,receive send command in listen
            connClient->startListen();
            connections.insert({e.first,connClient});
        }

    }

    void DataSender::send(DB::Block &block) {

        // shuffle block by distributekey and send to all executors

        std::map<UInt32,Block> toSend =  repartitionByKey(block);

        for(auto partion2block : toSend){
            auto it = connections.find(partion2block.first);
            assert(it!=connections.end());
            it->second->sendBlock(partion2block.second);
        }
    }

    std::map<UInt32,Block> DataSender::repartitionByKey(Block & block) {

        std::map<UInt32, Block> blocks; // partition id - > block
        if (dest.receiverInfo.size() == 1) {
            blocks.insert({0, block});
        } else {
            throw Exception("not impl yet");
        }

        return  blocks;


    }
}

