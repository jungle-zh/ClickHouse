//
// Created by Administrator on 2019/5/1.
//

#pragma once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/PlanNode/ExchangeNode.h>

namespace DB {

struct TaskInfo {

    std::string taskId;
    std::string outPutNodeId;

};
class Task {


    public:

        Task(std::shared_ptr<PlanNode>  rootNodes_, std::string type_
                ,int parallel_ = 0,int index_ = 0 , std::string taskId_ = ""
        ) : nodes(rootNodes_),type(type_),parallel(parallel_),index(index_),taskId(taskId_) {

                info.taskId = taskId_;
        }

        void executeTask();



        Block pullFromInput();


        void pushToOutPuts(Block & block);

        static  void serialize(WriteBuffer & buffer);


        static  void deserialize(ReadBuffer & buffer);

        void setOutPut(std::shared_ptr<ExchangeNode> outPut);




    private:
        std::shared_ptr<PlanNode> nodes;

        std::shared_ptr<ExchangeNode> input;

        std::vector<std::shared_ptr<ExchangeNode>> outputs;
        std::string type ;

        int parallel;
        int index ;
        std::string taskId;

        TaskInfo info;
};





}