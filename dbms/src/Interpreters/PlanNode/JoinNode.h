//
// Created by Administrator on 2019/3/31.
//

#pragma  once

#include <Interpreters/PlanNode/PlanNode.h>
#include <Interpreters/Join.h>
namespace DB {



class  JoinNode : public PlanNode {

private:

    std::shared_ptr<Join> join;
public:
    JoinNode(std::shared_ptr<Join> & join_):join(join_) {}
    Block read() override ;
    void prepareRightTable();

    bool prepared;


};

}
