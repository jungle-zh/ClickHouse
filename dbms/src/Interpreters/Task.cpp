//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataReceiver.h>
#include <Interpreters/DataSender.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include "Task.h"
namespace DB {


    void Task::setExechangeDest(ExechangeTaskDataDest & dest ) {
        exechangeTaskDataDest = dest ;

    }
    void Task::setScanSource(ScanTaskDataSource & source){
        scanTaskDataSource = source ;

    }
    void Task::setExechangeSource(ExechangeTaskDataSource & source){


        exechangeTaskDataSource = source ;
        //exechangeTaskDataSources.insert({source.childStageId,source});
        //childStageIds.push_back(source.childStageId);
    }

    void Task::init(){

        if(!isResultTask()){
            sender = std::make_shared<DataSender>(exechangeTaskDataDest);
            sender->tryConnect();  //block until success
        }
        receiver = std::make_shared<DataReceiver>(exechangeTaskDataSource,this);

        receiver->init();
        // if has join then call receiveHashTable until read all  to HashTable

        createBottomExecNodeByBuffer();

    }



    std::shared_ptr<DataReceiver> findHashTableReceiver(){
        return  std::shared_ptr<DataReceiver>();
    }


    void Task::execute(){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
            sender->send(res);           // logic thread  may be block int sendData if upstream buffer rich high waiter mark
        }
    }
    void Task::finish(){

    }

    void Task::receiveHashTable(Block &block) {

        JoinExecNode * joinExecNode = typeid_cast<JoinExecNode*>( root.get() );

        assert(joinExecNode != NULL);
        joinExecNode->getJoin()->insertFromBlock(block);

    }

    void Task::receiveBlock(Block &block) { // receive block and put to buffer


    }

    bool Task::highWaterMark() { // when receive buffer is more than 80%

    }

    void Task::createBottomExecNodeByBuffer(){

    }

}