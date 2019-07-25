//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/PlanNode.h>
#include <Core/SortDescription.h>
#pragma  once
namespace DB{


class SortMergeNode : public PlanNode {


public:


    SortDescription description;
    size_t limit ;

public:




};


}