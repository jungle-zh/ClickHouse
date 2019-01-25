//
// Created by admin on 19/1/23.
//

#include "ExecNodeBlockInputStream.h"

namespace DB {
Block  ExecNodeBlockInputStream::readImpl() {


    Block res  =  node->get_next();

    return  res;


}

}
