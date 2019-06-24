//
// Created by usser on 2019/6/17.
//

#pragma once

//#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/Partition.h>


namespace DB {


class DataSender;
class DataReceiver;
class ExecNode;


    class Task {
    public:
        void init();  // start receiver
        void prepareHashTable();
        void execute();
        void finish();
        std::shared_ptr<ExechangeTaskDataSource>  getExecSource() { return  exechangeTaskDataSource;}
        std::shared_ptr<ExechangeTaskDataDest> getExecDest() { return exechangeTaskDataDest; }
        std::shared_ptr<ScanTaskDataSource> getScanSource() { return scanTaskDataSource;}
        Task(ExechangeTaskDataSource source, ExechangeTaskDataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);


    private:
        DataBuffer &  buffer; //use when is result task , from server
        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;
        std::unique_ptr<DataReceiver> receiver;
        std::unique_ptr<DataSender> sender;

        Block inputHeader;
        std::shared_ptr<ExechangeTaskDataSource> exechangeTaskDataSource;
        std::shared_ptr<ExechangeTaskDataDest> exechangeTaskDataDest;
        std::shared_ptr<ScanTaskDataSource> scanTaskDataSource;

    };


}