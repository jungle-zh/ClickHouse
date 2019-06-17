//
// Created by Administrator on 2019/3/31.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>


namespace DB {

class ScanPlanNode : public PlanNode {


private:

// info about how to  create storageMergeTree

// part of StorageMergeTree::read result
BlockInputStreamPtr  table;

size_t index ;

public:

Block read() override ;



};

}

