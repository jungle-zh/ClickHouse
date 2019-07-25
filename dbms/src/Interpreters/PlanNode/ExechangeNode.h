//
// Created by admin on 19/1/20.
//

#pragma  once

#include <Poco/Net/TCPServer.h>
#include "PlanNode.h"

namespace DB {

// receive data from lower task and hold for upper task to read


class ExechangeNode  : public PlanNode {


private:

    DataExechangeType  type  ;
    std::shared_ptr<Distribution> distribution;

    // std::vector<std::shared_ptr<Distribution>> senderDistributions;
public:

    ExechangeNode(DataExechangeType type_ ,std::shared_ptr<Distribution> distribution_){
        type  = type_;
        distribution = distribution_;
    }
    DataExechangeType getDateExechangeType(){
        return  type;
    }




};




}
