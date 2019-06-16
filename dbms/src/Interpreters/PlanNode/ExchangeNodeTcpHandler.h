//
// Created by Administrator on 2019/5/4.
//
#pragma  once
#include <Poco/Net/TCPServerConnection.h>
#include <Interpreters/PlanNode/ExechangeNode.h>
#include <Interpreters/PlanNode/BlockLocalContainer.h>
namespace DB {
class   ExchangeNodeTcpHandler : public Poco::Net::TCPServerConnection {
 private:

    ExechangeNode & node ;
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    UInt64 packageType ;

    std::string  tableName;
    BlockLocalContainer blockLocalContainer;
public:
     ExchangeNodeTcpHandler(ExechangeNode & node_, const Poco::Net::StreamSocket socket)
     :node(node_),Poco::Net::TCPServerConnection(socket){


     }

    void run() override; // when run is finished , ExchangeNodeTcpHandler will be destory ,see TCPServerDispatcher line 111


    void runImpl();

    Block read() ;


    std::string getTableName(){
         return tableName;
    }

};
}