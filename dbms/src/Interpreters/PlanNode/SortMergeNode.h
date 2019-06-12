//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/PlanNode.h>
#include <Core/SortDescription.h>
#pragma  once
namespace DB{


class SortMergeNode : public PlanNode {


private:


    SortDescription description;
    size_t limit ;
    BlockInputStreams inputs_to_merge;
    std::unique_ptr<IBlockInputStream> impl;
    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;
public:

    SortPartialNode(SortDescription && description_,size_t limit_) :
    description(description_),limit(limit_){

    }

    void init () override;

    Block read() override ;


};


}