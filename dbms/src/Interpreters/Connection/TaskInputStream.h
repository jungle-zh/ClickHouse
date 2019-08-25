//
// Created by jungle on 19-6-18.
//

#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Interpreters/Task.h>

namespace DB {

    class Context;
    class TaskInputStream  {


    public:
       // void init();
        std::shared_ptr<Task> read();
        TaskInputStream(std::shared_ptr<ReadBuffer> in_  , int version_ , Context * context_){
            in = in_;
            version = version_;
            context = context_;
        }

    private:
        //ExechangeTaskDataSource readTaskExechangeSource();
        //ExechangePartition readExechangePartition();
        //ScanTaskDataSource readTaskScanSource();
        ScanPartition readScanPartition();
        scanTableInfo readScanTableInfo();
        TaskSource readTaskSource();
        StageSource readStageSource();
        ScanSource readScanSource();
        Distribution readFatherDistribution();

        std::shared_ptr<ExecNode> readExecNode();

        std::shared_ptr<Poco::Net::StreamSocket> socket;
        std::shared_ptr<ReadBuffer> in;
        int version;
        Context * context;
    };


}
