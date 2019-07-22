//
// Created by jungle on 19-6-16.
//

#pragma once

#include "ExecNode.h"

namespace DB {

    class ScanExecNode : public ExecNode{

    public:

        void serialize(WriteBuffer & buffer);
        static  std::shared_ptr<ScanExecNode> deseralize(ReadBuffer & buffer);

        Block read() override;


    };

}



