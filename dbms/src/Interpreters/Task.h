//
// Created by usser on 2019/6/17.
//

#pragma once

//#include <Interpreters/ExecNode/ExecNode.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Partition.h>
#include <common/logger_useful.h>


namespace DB {

class Block;
class DataSender;
class DataReceiver;
class ExecNode;
class Stage ;
class JoinExecNode;
class IBlockInputStream;
class  DataConnectionHandlerFactory;
class  Context;


    class Task {
    public:

        Task(std::string taskId_ ,Context * context_);
        Task(ExechangeTaskDataSource source,ScanTaskDataSource source1,
             ExechangeTaskDataDest dest , std::vector<std::shared_ptr<ExecNode>> nodes,std::string taskId_,Context * context);
        void init();  // start receiver
        void initFinal( );
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
        void addFinishChildTask(std::string childTaskId) { finishedChildTask.push_back(childTaskId);}
        void addChildTask(std::string childTaskId) { childTask.push_back(childTaskId);}
        bool allChildTaskFinish() { return finishedChildTask.size() ==  childTask.size(); }

        void createBottomExecNodeByBuffer();
        //void setDataConnectionHandlerFactory(DataConnectionHandlerFactory * factory_){  dataConnectionHandlerFactory = factory_;}



    private:
        //DataBuffer &  buffer; //use when is result task , from server
        std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;
       // std::shared_ptr<ConcurrentBoundedQueue<Block>> resultBuffer;

        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;


        std::shared_ptr<DataSender> sender;
        std::shared_ptr<DataReceiver> receiver ;

        ExechangeTaskDataSource exechangeTaskDataSource;
        ExechangeTaskDataDest exechangeTaskDataDest;
        ScanTaskDataSource scanTaskDataSource;


        int partionId;
        std::string taskId ;
        std::string taskType ;
        bool  isScanTask_ = false ;
        bool  isResultTask_ = false;

        Poco::Logger *log  ;
        Context * context;
        std::vector<std::string> finishedChildTask;
        std::vector<std::string> childTask;
        //DataConnectionHandlerFactory * dataConnectionHandlerFactory ;

    };


}