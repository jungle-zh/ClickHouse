//
// Created by Administrator on 2019/5/1.
//

#include <Interpreters/Exec/ExecutorProxy.h>


namespace DB {


void ExecutorProxy::submit(Task & task){

    // serialize info :
    //input exchange node info

    //output exchange nodes info

    //planNodes info


     WriteBuffer buf = std::make_shared<WriteBuffer>();
     task.info.serialize(buf);


     conn->send(buf);


}


}