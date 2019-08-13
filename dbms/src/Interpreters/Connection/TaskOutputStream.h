//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Interpreters/Task.h>
#include <IO/WriteBuffer.h>

namespace DB {


class TaskOutputStream {  //




public:

    TaskOutputStream(std::shared_ptr<WriteBuffer> out_ ,int version_ ){
        out = out_;
        version = version_;
    }
    
public:
    void init();
    void write(Task &  task);
    void write(ExechangeTaskDataSource & source);
    void write(ExechangePartition & partition);
    void write(DataReceiverInfo & info);
    void write(ExechangeTaskDataDest & desc );
    void write(ScanTaskDataSource & source);
    void write(ScanPartition & partition);
    void write(scanTableInfo & info);
    void write(std::vector<std::shared_ptr<ExecNode>> execnodes);
    void write(std::shared_ptr<ExecNode> execNode);

private:
    //std::shared_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<WriteBuffer> out;
    int version;

};



}

