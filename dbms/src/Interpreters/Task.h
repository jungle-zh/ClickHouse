//
// Created by usser on 2019/6/17.
//

#pragma once

//#include <Interpreters/ExecNode/ExecNode.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Partition.h>


namespace DB {

class Block;
class DataSender;
class DataReceiver;
class ExecNode;
class Stage ;
class JoinExecNode;
class IBlockInputStream;



    class Task {
    public:

        Task(){};
        Task(ExechangeTaskDataSource source,ExechangeTaskDataDest dest , std::vector<std::shared_ptr<ExecNode>> nodes);
        void init();  // start receiver
        void execute();
        void execute(std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer);
        void finish();
        void setScanTask() {isScanTask_ = true;}
        void setResultTask() {isResultTask_ = true;}
        bool isScanTask(){ return  isScanTask_; }
        bool isResultTask() { return  isResultTask_;}
        void setExechangeSource(ExechangeTaskDataSource & source);
        void setExechangeDest(ExechangeTaskDataDest & dest);
        void setScanSource(ScanTaskDataSource & source);
        void setExecNodes(std::vector<std::shared_ptr<ExecNode>> execNodes_){  execNodes = execNodes_;}
        JoinExecNode * getJoinExecNode();
        ExechangeTaskDataSource   getExecSources() { return  exechangeTaskDataSource;}
        ExechangeTaskDataDest  getExecDest() { return exechangeTaskDataDest; }
        ScanTaskDataSource getScanSource() { return scanTaskDataSource;}
        std::vector<std::shared_ptr<ExecNode>> getExecNodes() { return  execNodes ;}
        std::string getTaskId() { return taskId;}
        //Task(ExechangeTaskDataSource source, ExechangeTaskDataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);
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
        bool  isScanTask_ = false ;
        bool  isResultTask_ = false;


    };


}