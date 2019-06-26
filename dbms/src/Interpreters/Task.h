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
        void prepareHashTable(std::shared_ptr<DataReceiver> receiver);
        void execute();
        void finish();
        std::vector<std::shared_ptr<ExechangeTaskDataSource>>   getExecSources() { return  exechangeTaskDataSources;}
        std::shared_ptr<ExechangeTaskDataDest> getExecDest() { return exechangeTaskDataDest; }
        std::shared_ptr<ScanTaskDataSource> getScanSource() { return scanTaskDataSource;}
        Task(ExechangeTaskDataSource source, ExechangeTaskDataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);


    private:
        DataBuffer &  buffer; //use when is result task , from server
        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;

        std::shared_ptr<DataSender> sender;

        Block inputHeader;
        std::map<int,std::shared_ptr<ExechangeTaskDataSource>> exechangeTaskDataSources;
        std::map<int,std::shared_ptr<DataReceiver>> receivers; // one table one receiver

        std::vector<int> childStageIds;
        std::shared_ptr<ExechangeTaskDataDest> exechangeTaskDataDest;
        std::shared_ptr<ScanTaskDataSource> scanTaskDataSource;
        DataExechangeType exechangeType;

    };


}