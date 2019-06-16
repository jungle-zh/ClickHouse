//
// Created by admin on 19/1/20.
//

#include <Interpreters/PlanNode/ExechangeNode.h>
#include <Interpreters/PlanNode/ExchangeNodeTcpHandler.h>
#include <Interpreters/PlanNode/ExchangeNodeTcpHandlerFactory.h>

namespace DB {

void ExechangeNode::init(){


   server = new Poco::Net::TCPServer(
            new ExchangeNodeTcpHandlerFactory(*this),
            server_pool,
            socket,
            new Poco::Net::TCPServerParams)

   server->start();

}

void ExechangeNode::addHandler(std::shared_ptr<ExchangeNodeTcpHandler> handler) {

   handlers.push_back(handler);
}

Block ExechangeNode::read() {

    if(type == "shuffleJoin"){
        for(std::shared_ptr<ExchangeNodeTcpHandler>  conn : handlers){
            if(conn->getTableName() == leftTableName ){
                return  conn->read();
            }
        }
    } else if( type == "aggMerge" || type == "sortMerge" ){

        if(currentHandlerIndex == handlers.size())
            return  Block();
        Block  block = handlers[currentHandlerIndex]->read();
        if(block)
            return  block
        else
            return  handlers[++currentHandlerIndex]->read();

    }



}

Block ExechangeNode::readRightTable() {}


    for(std::shared_ptr<ExchangeNodeTcpHandler>  conn : handlers){
         if(conn->getTableName() == rightTableName ){
            return  conn->read();
         }
    }
}