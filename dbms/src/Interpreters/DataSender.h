//
// Created by usser on 2019/6/17.
//

#pragma once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Client/Connection.h>
#include <Interpreters/Task.h>


namespace DB {


class Block;

class DataSender {

public:

    DataSender(DataDest &  dest_ );

    DataDest dest;


    std::map<UInt32 , partitionInfo> partitions; // partitionid -> info
    std::map<UInt32 , Connection> connections;   // partitionid -> connnections



    void send(Block & block);

    std::map<UInt32,Block> repartitionByKey(Block & block);
    void addConnection();


};


}