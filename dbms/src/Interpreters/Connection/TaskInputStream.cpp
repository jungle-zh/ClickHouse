//
// Created by jungle on 19-6-18.
//

#include <Interpreters/ExecNode/ExecNode.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include "TaskInputStream.h"

namespace  DB {

    void TaskInputStream::init() {
        in  = std::make_shared<ReadBufferFromPocoSocket>(*socket);
    }


    std::shared_ptr<ExecNode> TaskInputStream::readExecNode() {

        int nodeType ;
        readVarInt(nodeType, *in);
        switch (nodeType) {

            case ExecNode::NodeType::TAgg : {

                return AggExecNode::deserialize(*in);

            }


        }

    }

    ExechangeTaskDataDest TaskInputStream::readTaskDest() {


    }

    ExechangeTaskDataSource TaskInputStream::readTaskSource() {

    }

    std::shared_ptr<Task> TaskInputStream::read(){


        auto source  = readTaskSource();
        auto dest = readTaskDest();
        int ExecNodeNum ;
        readVarInt(ExecNodeNum,*in);
        std::vector<ExecNode> nodes;
        for(int i=0;i < ExecNodeNum ;++i){
           auto node =  readExecNode();
            nodes.emplace_back(node);
        }

        return std::make_shared<Task>(source,dest,nodes);

    }







}