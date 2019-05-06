//
// Created by Administrator on 2019/5/1.
//

#pragma once


namespace DB {



class Stage {

private:

   // std::vector<> nodes;
    std::shared_ptr<PlanNode> topNode;
    std::vector<std::shared_ptr<Stage>> childs;

    std::vector<ExchangeNode> inputs;

    std::vector<ExchangeNode> outputs;

    std::vector<std::shared_ptr<Task>> tasks;

public:

    Stage(int stageId):stageId(stageId){

    }
    std::vector<std::shared_ptr<Stage>> getChilds() { return childs; };
    std::string  type();
    void addNode(std::shared_ptr<PlanNode> node);


    void addChild(std::shared_ptr<Stage> child);


    void addInput(std::shared_ptr<ExchangeNode> input);


    void addOutput(std::shared_ptr<ExchangeNode> output);

    void buildResultTask();

    void buildScanTask();

    void buildTask();

    int getHashPartitionNum();

    void whereToSubmitTasks();
    void submitTasks();


    int stageId  ;
    int taskCnt ;


};


}


