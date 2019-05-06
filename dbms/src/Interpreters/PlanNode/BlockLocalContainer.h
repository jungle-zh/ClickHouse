//
// Created by Administrator on 2019/5/4.
//

#pragma  once
namespace DB {


class BlockLocalContainer {

    bool write(Block & block);

    bool read(Block & block);

};


}