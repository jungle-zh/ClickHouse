//
// Created by Administrator on 2019/5/1.
//

#pragma  once

#include <Interpreters/Exec/Task.h>
#include <Client/Connection.h>
namespace DB {



class  ExecutorProxy  {

public:
    void submit(Task & task);


   std::unique_ptr<Connection>  conn ;

};





}