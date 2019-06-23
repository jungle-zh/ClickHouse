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

    std::string type  ;


   // std::vector<std::shared_ptr<Distribution>> senderDistributions;
public:

  //  std::shared_ptr<Distribution> getSenderDistribution();
  //  void addSenderDistribution(std::shared_ptr<Distribution>  sender){ senderDistributions.push_back(sender);}




};




}
