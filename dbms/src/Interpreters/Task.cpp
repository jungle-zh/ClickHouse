//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataReceiver.h>
#include <Interpreters/DataSender.h>
#include "Task.h"
namespace DB {


    void Task::init(){

        inputHeader = execNodes.back()->getHeader();
        if(dataSource.type == DataSourceType::exechange) {
            receiver = std::make_unique<DataReceiver>(dataSource,inputHeader);
            receiver->startToReceive();
        }
        sender = std::make_unique<DataSender>(dataDest);
    }
    void Task::execute(){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
            sender->send(res);
        }
    }
    void Task::finish(){

    }

}