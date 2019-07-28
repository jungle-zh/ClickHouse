//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataReceiver.h>
#include <Interpreters/DataSender.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/TaskReceiverExecNode.h>
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

        if(!exechangeTaskDataDest.isResult){
            sender = std::make_shared<DataSender>(exechangeTaskDataDest);
            sender->tryConnect();  //block until success
        }
        receiver = std::make_shared<DataReceiver>(exechangeTaskDataSource); // will create tcp server and accept connection

        receiver->init();  // if has join then call receiveHashTable until read all  to HashTable


        createBottomExecNodeByBuffer();

    }



    std::shared_ptr<DataReceiver> findHashTableReceiver(){
        return  std::shared_ptr<DataReceiver>();
    }


    void Task::execute(){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
             sender->send(res);           // logic thread  may be block in sendData if upstream buffer rich high waiter mark
        }
    }

    void Task::execute(std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
            buffer->push(res);
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

        buffer->push(block);

    }

    bool Task::highWaterMark() { // when receive buffer is more than 80%
        return  ( (float)(buffer->size()) / (float)(buffer->max())) > 0.8;
    }

    void Task::createBottomExecNodeByBuffer(){

        std::shared_ptr<TaskReceiverExecNode> node = std::make_shared<TaskReceiverExecNode>(buffer)  ;
        auto  cur = root;
        auto  pre = root ;
        while(cur){
            pre = cur ;
            cur = cur->getChild();
        }
        pre->setChild(node);

    }


}