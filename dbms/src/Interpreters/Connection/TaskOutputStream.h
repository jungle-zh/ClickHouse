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
    void  writeTaskSource(DataSource source);
    void  writeTaskDest(DataDest dest);
    void  writeExecNode(std::shared_ptr<ExecNode> ndoe);


    std::shared_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<WriteBuffer> out;


public:
    void init();
    void write(Task &  task);
    static void write(ExechangeTaskDataSource & source);
    static void write(ExechangeTaskDataDest & desc );
    static void write(ScanTaskDataSource & source);
    static void write(std::vector<std::shared_ptr<ExecNode>> execnodes);

private:
    std::shared_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<WriteBuffer> out;

};



}

