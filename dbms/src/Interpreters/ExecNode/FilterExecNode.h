//
// Created by jungle on 19-6-16.
//
#pragma once

#include <Columns/FilterDescription.h>
#include "ExecNode.h"

namespace DB {


class FilterExecNode  : public  ExecNode{


public:

    FilterExecNode(std::string filter_column_name_   , ExpressionActionsPtr expression_ ){
        filter_column_name = filter_column_name_;
        expression = expression_;
    }
    std::string filter_column_name;
    ExpressionActionsPtr expression;


    ssize_t filter_column;
    Block header ;
    Block inputHeader ;
    ConstantFilterDescription constant_filter_description;



    void  readPrefix() override;
    Block read() override ;
    Block getHeader() override {

        Block header = children->getHeader();
        expression->execute(header);
        return header ;
    }

    Block getInputHeader() override { return  children->getHeader();}


    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;



};



}