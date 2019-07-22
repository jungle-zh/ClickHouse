//
// Created by usser on 2019/6/17.
//

#pragma once

//#include <Interpreters/ExecNode/ExecNode.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Partition.h>


namespace DB {


class DataSender;
class DataReceiver;
class ExecNode;
class Stage ;
class JoinExecNode;
class IBlockInputStream;



    class Task {
    public:

        void init();  // start receiver
        void execute();
        void execute(std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer);
        void finish();
        void setExechangeSource(ExechangeTaskDataSource & source);
        void setExechangeDest(ExechangeTaskDataDest & dest);
        void setScanSource(ScanTaskDataSource & source);
        bool isResultTask(); // only has receiver;
        JoinExecNode * getJoinExecNode();
        ExechangeTaskDataSource   getExecSources() { return  exechangeTaskDataSource;}
        ExechangeTaskDataDest  getExecDest() { return exechangeTaskDataDest; }
        ScanTaskDataSource getScanSource() { return scanTaskDataSource;}
        std::vector<std::shared_ptr<ExecNode>> getExecNodes() { return  execNodes ;}
        std::string getTaskId() { return taskId;}
        Task(ExechangeTaskDataSource source, ExechangeTaskDataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);
        DataExechangeType exechangeType;

        void receiveHashTable(Block &block);

        void receiveBlock(Block &block);

        bool highWaterMark();

        void createBottomExecNodeByBuffer();



    private:
        //DataBuffer &  buffer; //use when is result task , from server
        std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;

        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;


        std::shared_ptr<DataSender> sender;
        std::shared_ptr<DataReceiver> receiver ;

        ExechangeTaskDataSource exechangeTaskDataSource;
        ExechangeTaskDataDest exechangeTaskDataDest;
        ScanTaskDataSource scanTaskDataSource;


        int partionId;
        std::string taskId ;
        bool  isScanTask ;


    };


}