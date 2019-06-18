//
// Created by usser on 2019/6/17.
//

#include <Interpreters/DataSender.h>

namespace DB {


    void DataSender::send(DB::Block &block) {

        // shuffle block by distributekey and send to all executors

        std::map<UInt32,Block> toSend =  repartitionByKey(block);

        for(auto send : toSend){
            connections[send.first].sendData(send.second);
        }
    }

    std::map<UInt32,Block> DataSender::repartitionByKey(Block & block){

        std::map<UInt32,Block> blocks ; // partition id - > block



    }
}

