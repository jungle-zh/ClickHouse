//
// Created by usser on 2019/6/17.
//

#pragma once

//#include <Interpreters/ExecNode/ExecNode.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Interpreters/Partition.h>
#include <common/logger_useful.h>
#include <common/ThreadPool.h>


namespace DB {

class Block;
class DataExechangeClient;
class DataExechangeServer;
class ExecNode;
class Stage ;
class JoinExecNode;
class IBlockInputStream;
class  DataConnectionHandlerFactory;
class  Context;


    class Task {
    public:

        Task(std::shared_ptr<Distribution> fatherDistribution,std::map<std::string  , StageSource>  stageSource, ScanSource scanSource
                ,std::string taskId,std::vector<std::string> mainTableStageIds_ ,std::vector<std::string> hashTableStageIds_,Context * context_);
        Task(std::shared_ptr<Distribution>  fatherDistribution,std::map<std::string  , StageSource>  stageSource, ScanSource scanSource,
                std::vector<std::shared_ptr<ExecNode>> nodes,std::string taskId_,std::vector<std::string> mainTableStageIds_ ,std::vector<std::string> hashTableStageIds_
                ,bool hasScan,bool hasExechage,bool isResult,Context * context);
        ~Task(){
            LOG_DEBUG(log,"task destory :" + taskId);
        }
        void init();  // start receiver
        void checkStageSourceDistribution();
        void initFinal( );
        void consume();
        void produce();
        void execute();
        void execute(std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer);
        void finish();

        void setExecNodes(std::vector<std::shared_ptr<ExecNode>> execNodes_){  execNodes = execNodes_;}
        //std::shared_ptr<ConcurrentBoundedQueue<Block>> getPartitionBuffer(size_t destPartitionId);
        std::vector<std::shared_ptr<ExecNode>> getExecNodes() { return  execNodes ;}
        std::string getTaskId() { return taskId;}
        //Task(ExechangeTaskDataSource source, ExechangeTaskDataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);
        DataExechangeType exechangeType;

        bool  hasScan = false;
        bool  hasExechange = false;
        bool  isResult  = false;
        std::shared_ptr<Distribution> fatherDistribution;
        std::map<std::string  , StageSource> stageSource ; // stageSource's
        ScanSource scanSource;



        bool highWaterMark();
        void addFinishChildTask(std::string childTaskId) { finishedChildTask.push_back(childTaskId);}
        void addChildTask(std::string childTaskId) { childTask.push_back(childTaskId);}
        bool allChildTaskFinish() { return finishedChildTask.size() ==  childTask.size(); }

        void createExecNodeByClient();
        void debugString(std::stringstream  & ss ,size_t blankNum );
        void debugDest(std::stringstream  & ss);
        std::map<UInt32, Block> repartition(Block block);
        TaskSource getExechangeServerInfo();
        //void setDataConnectionHandlerFactory(DataConnectionHandlerFactory * factory_){  dataConnectionHandlerFactory = factory_;}
        std::vector<std::string> split1(const std::string& str, const std::string& delim) ;

        std::vector<std::string> mainTableStageIds;
        std::vector<std::string> hashTableStageIds;

        TaskSource myServer;
        bool  isInited ;

    private:

        std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer;
        std::map<UInt32 ,std::shared_ptr<ConcurrentBoundedQueue<Block>> > partitionBuffer;

        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;


        std::shared_ptr<DataExechangeClient> client;
        std::shared_ptr<DataExechangeServer> server ;


        ThreadPool pool{2};

        size_t partionId;

        std::string taskId ;

        bool  taskFinished = false;

        Poco::Logger *log  ;
        Context * context;
        std::vector<std::string> finishedChildTask;
        std::vector<std::string> childTask;
        std::mutex hashTableLock;
        std::mutex mainTableLock;



    };


}