//
// Created by usser on 2019/6/15.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {



class JoinExecNode : public ExecNode {


private:
    Names joinKey;
    Block inputLeftHeader;
    Block inputRightHeader;


};



}