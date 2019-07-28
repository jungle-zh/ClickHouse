//
// Created by jungle on 19-6-16.
//

#pragma once

#include "ExecNode.h"

namespace DB {

    class ScanExecNode : public ExecNode{

    public:

        void serialize(WriteBuffer & buffer){ (void)buffer;}
        static  std::shared_ptr<ScanExecNode> deseralize(ReadBuffer & buffer){
            (void) buffer;
            return std::shared_ptr<ScanExecNode>(); }

        Block read() override{ return  Block();}

         void  readPrefix() override {};
         void  readSuffix() override {};
         Block getHeader (bool isAnalyze)  override { (void) isAnalyze;return  Block();};
         Block getInputHeader()  override { return  Block();};


    };

}



