//
// Created by usser on 2019/6/17.
//

#include "Task.h"


namespace DB {


    void Task::init(){

        receiver->startToReceive();
        //start receiver node ;
    }
    void Task::execute(){

        while(Block res = root->read()){ // read all until child send empty block
            sender->send(res);
        }
    }
    void Task::finish(){

    }

}