//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataReceiver.h>
#include <Interpreters/DataSender.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include "Task.h"
namespace DB {


    void Task::init(){

        inputHeader = execNodes.back()->getHeader();
        for(auto  e : exechangeTaskDataSources) {
            auto receiver = std::make_shared<DataReceiver>(*(e.second),inputHeader);
            receiver->startToAccept();
            receivers.insert({e.first,receiver});
        }
        sender = std::make_shared<DataSender>(dataDest);


        if(exechangeType == DataExechangeType::tone2onejoin ||
           exechangeType == DataExechangeType::toneshufflejoin){
            assert(childStageIds.size() == 1);
            prepareHashTable(receivers[childStageIds[0]]);
        }else if(exechangeType == DataExechangeType::ttwoshufflejoin){
            assert(childStageIds.size() == 2);
            std::shared_ptr<DataReceiver> hash_receiver = findHashTableReceiver();
            prepareHashTable(hash_receiver);
        }

        for(auto e : receivers){
            e.second->setStartToReceive(true);
        }



    }


    std::shared_ptr<DataReceiver> findHashTableReceiver(){
        return  std::shared_ptr<DataReceiver>();
    }
    void Task::prepareHashTable(std::shared_ptr<DataReceiver> receiver){

        receiver->setStartToReceive(true);
        JoinExecNode * node = getJoinExecNode();

        while(1){
            Block block = receiver->read();
            if(!block)
                break;

            node->getJoin()->insertFromBlock(block);
        }

    }

    void Task::execute(){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
            sender->send(res);
        }
    }
    void Task::finish(){

    }

}