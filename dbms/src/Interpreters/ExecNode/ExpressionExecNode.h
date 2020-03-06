//
// Created by jungle on 19-7-21.
//

#include "ExecNode.h"

namespace DB {



class ExpressionExecNode  : public  ExecNode{

public:


    ExpressionExecNode(std::shared_ptr<ExpressionActions> expression_){
        expression = expression_;
    }
    void  readPrefix(std::shared_ptr<DataExechangeClient> client) override {(void)client;};
    void  readSuffix() override {};
    Block read() override ;
    Block getHeader(bool  isAnalyze) override ;
    Block getInputHeader() override ;
    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer,Context * context) ;

    std::shared_ptr<ExpressionActions> expression ;
    std::string getName() override { return  "expressionExecNode";}

};

}