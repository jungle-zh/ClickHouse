//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Interpreters/Task.h>
#include <IO/WriteBuffer.h>

namespace DB {


class TaskOutputStream {  //




private:
    void  writeTaskSource(ExechangeTaskDataSource source);
    void  writeTaskDest(ExechangeTaskDataDest dest);
    void  writeExecNode(std::shared_ptr<ExecNode> ndoe);
    TaskOutputStream(std::shared_ptr<WriteBuffer> out_ ,int version_ ){
        out = out_;
        version = version_;
    }
    
public:
    void init();
    void write(Task &  task);
    void write(ExechangeTaskDataSource & source);
    void write(ExechangeTaskDataDest & desc );
    void write(ScanTaskDataSource & source);
    void write(std::vector<std::shared_ptr<ExecNode>> execnodes);
    void write(std::shared_ptr<ExecNode> execNode);

private:
    //std::shared_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<WriteBuffer> out;
    int version;

};



}

