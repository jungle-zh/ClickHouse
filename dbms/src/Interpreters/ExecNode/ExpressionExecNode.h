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
    Block read() override ;
    Block getHeader() override ;
    Block getInputHeader() override ;
    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;

    std::shared_ptr<ExpressionActions> expression ;

};

}