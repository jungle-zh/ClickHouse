//
// Created by admin on 19/1/20.
//

#pragma  once

#include <Poco/Net/TCPServer.h>
#include "PlanNode.h"

namespace DB {

// receive data from lower task and hold for upper task to read

class ExchangeNodeTcpHandler;
class ExechangeNode  : public PlanNode {


private:
    std::unique_ptr<Poco::Net::TCPServer> server;
    std::vector<std::shared_ptr<ExchangeNodeTcpHandler>> handlers;
    std::string rightTableName ;
    std::string leftTableName ;
    std::string type  ;
    size_t currentHandlerIndex ;

    std::shared_ptr<PlanNode::Distribution> distribution;
public:

    Block readRightTable(std::string table) ;
    void  addHandler(std::shared_ptr<ExchangeNodeTcpHandler> handler);
    static void serialize(WriteBuffer & buffer);

    static void deserialize(ReadBuffer & buffer);





};




}
