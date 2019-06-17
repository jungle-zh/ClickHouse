//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/Stage.h>
#include <Client/Connection.h>
namespace DB {


class TaskScheduler {


public:
    std::vector<executorId> applyExecutorsForStage(std::shared_ptr<Stage> stage);
    void submitStage(std::shared_ptr<Stage> root);

    std::map<int,Connection> executorConnection;



};


}


