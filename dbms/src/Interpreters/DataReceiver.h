//
// Created by usser on 2019/6/17.
//

#pragma once

namespace DB {



class DataReceiver {


    void startReceiver(); // receive and deserialize
    Block read();

};


}

