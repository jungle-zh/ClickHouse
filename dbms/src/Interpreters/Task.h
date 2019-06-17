//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {



class Task {


    std::vector<std::shared_ptr<ExecNode>> execNodes;

    std::shared_ptr<ExecNode> root;
    std::shared_ptr<ExecNode> receiver;
    std::shared_ptr<ExecNode> sender;
    static  void serialize(WriteBuffer & buffer);
    static  Task deSerialize(ReadBuffer & buffer); //


    void init();  // start receiver node
    void execute();
    void finish();




};




}