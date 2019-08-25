//
// Created by usser on 2019/6/17.
//

#include <Interpreters/DataExechangeClient.h>

namespace DB {

    void DataExechangeClient::tryConnectAll(){



        for(auto stageSource: source){

               for(auto pair :stageSource.second.taskSources){
                   TaskSource taskSource = pair.second;
                   auto connClient =  std::make_shared<DataConnectionClient>(taskSource.ip,taskSource.dataPort,task,context);
                   connClient->connect();  // try until success
                   connClient->sendPartitionId(task->getTaskId(),partitionId);
                   auto it =  connections.find(stageSource.first);
                   if(it == connections.end()){
                       std::map<std::string ,std::shared_ptr<DataConnectionClient>>  stageConnecions;
                       stageConnecions.insert({taskSource.taskId,connClient});   //jungle comment : it is child taskId from StageSource
                       connections.insert({stageSource.first,stageConnecions});
                   } else {
                       it->second.insert({taskSource.taskId,connClient});
                   }
               }


        }

    }
    Block DataExechangeClient::read(std::string stageId){

       auto stageConnecions =  connections.find(stageId);
       if(stageConnecions ==connections.end() )
           throw Exception("not find stage in connections");


       for(auto taskConnection :stageConnecions->second ){

            if(finishedTaskConnection.find(taskConnection.first) == finishedTaskConnection.end()){
                Block res =  taskConnection.second->read(); // read corresponding partition buffer ;
                if(res)
                    return res;
                else
                    finishedTaskConnection.insert(taskConnection.first);
            }

       }

        Block empty;
        return  empty;

    }



    /*
    void DataExechangeClient::send(DB::Block &block) {

        // shuffle block by distributekey and send to all executors

        std::map<UInt32,Block> toSend =  repartitionByKey(block);

        for(auto partion2block : toSend){
            auto it = connections.find(partion2block.first);
            assert(it!=connections.end());
            it->second->sendBlock(partion2block.second);
        }
    }

    std::map<UInt32,Block> DataExechangeClient::repartitionByKey(Block & block) {

        std::map<UInt32, Block> blocks; // partition id - > block
        if (dest.receiverInfo.size() == 1) {
            blocks.insert({0, block});
        } else {
            throw Exception("not impl yet");
        }

        return  blocks;


    }
     */


}

