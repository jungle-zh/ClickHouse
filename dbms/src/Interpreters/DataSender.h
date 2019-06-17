//
// Created by usser on 2019/6/17.
//

#pragma once

namespace DB {



class DataSender {

    std::vector<executorId> executors;

    std::map<executorId,Connections> connections;

    void send(Block & block,executorId);


};


}