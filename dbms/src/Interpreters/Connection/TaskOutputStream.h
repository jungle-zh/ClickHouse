//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Interpreters/Task.h>

namespace DB {


class TaskOutputStream {  //


public:
    void init();
    void write(std::shared_ptr<Task> task );


private:
    void  writeTaskSource(DataSource source);
    void  writeTaskDest(DataDest dest);
    void  writeExecNode(std::shared_ptr<ExecNode> ndoe);

    std::shared_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<WriteBuffer> out;


};



}

