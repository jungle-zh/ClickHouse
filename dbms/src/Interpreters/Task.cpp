//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataExechangeServer.h>
#include <Interpreters/DataExechangeClient.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/DataClientExecNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include "Task.h"
#include "Stage.h"

namespace DB {


    Task::Task(Distribution fatherDistribution_,std::map<std::string,StageSource>  stageSource_, ScanSource scanSource_,
            std::string taskId_, Context *context_) {
        fatherDistribution = fatherDistribution_;
        stageSource = stageSource_;
        scanSource = scanSource_;
        taskId = taskId_;
        log = &Logger::get("Task");
        context = context_;
        buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
    };
    Task::Task(Distribution fatherDistribution_,std::map<std::string,StageSource>  stageSource_, ScanSource scanSource_,
               std::vector<std::shared_ptr<DB::ExecNode>> nodes, std::string taskId_,Context * context_) {
        fatherDistribution = fatherDistribution_;
        stageSource = stageSource_;
        scanSource = scanSource_;
        execNodes  = nodes;
        taskId  = taskId_;
        log = &Logger::get("Task");
        context = context_;
        std::shared_ptr<ExecNode> tmp = NULL;
        for(size_t i=0 ;i<nodes.size(); ++i){
            if(!tmp){
                tmp = nodes[i];
                root = tmp;
            } else{
                tmp->setChild(nodes[i]);
                tmp = nodes[i];
            }
        }
        buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
        for(auto partitionId : fatherDistribution.parititionIds){
            std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer = std::make_shared<ConcurrentBoundedQueue<Block>>();
            partitionBuffer.insert({partitionId,buffer});
        }

    }

    void Task::checkStageSourceDistribution(){
        if(stageSource.size() >0){

            if(exechangeType == DataExechangeType::tunion){

            } else if(exechangeType == DataExechangeType::toneshufflejoin
              || exechangeType == DataExechangeType::ttwoshufflejoin
              || exechangeType == DataExechangeType::tone2onejoin){

                std::vector<std::string> newDistributeKeys;
                std::vector<UInt32> newPartitionIds;
                bool isFirst = true;
                bool isSameDistribution = true;
                for(auto e : stageSource){
                    if(isFirst){
                        newDistributeKeys =  e.second.newDistributeKeys ;
                        newPartitionIds = e.second.newPartitionIds;
                        isFirst = false;
                    } else {
                        for(size_t  i=0 ;i < newDistributeKeys.size() ;++i){
                            if(newDistributeKeys[i] != e.second.newDistributeKeys[i]){
                                isSameDistribution = false;
                            }
                        }
                        for(size_t  i=0 ;i < newPartitionIds.size() ;++i){
                            if(newPartitionIds[i] != e.second.newPartitionIds[i]){
                                isSameDistribution = false;
                            }
                        }


                    }

                }
                if(!isSameDistribution){
                    throw Exception("join stage is not the same distribution");
                }

            }

        }

    }



    void Task::initFinal(){


        assert(root == NULL);
            std::shared_ptr<ExecNode> tmp = NULL;
            for(size_t i=0 ;i<execNodes.size(); ++i){
                if(!tmp){
                    tmp = execNodes[i];
                    root = tmp;
                } else{
                    tmp->setChild(execNodes[i]);
                    tmp = execNodes[i];
                }
            }

        //resultBuffer = io_buffer;
        init();
    }

    void Task::init(){



        if(!isResult){
            server = std::make_shared<DataExechangeServer>(partitionBuffer,this,context); // will create tcp server and accept connection
            server->init();
        }
        if(hasExechange)
            client = std::make_shared<DataExechangeClient>(stageSource,this,context);
        if(!hasScan)
            createExecNodeByClient();
        LOG_DEBUG(log,"task :" + taskId + " start to receive data and execute ");

        auto curNode = root;
        while(curNode){
            curNode->readPrefix(client); //joinNode need to prepare;
            curNode = curNode->getChild();
        }

    }


    std::map<UInt32, Block> Task::repartition(Block block){
        (void)block;
    }
    void Task::execute(){

        pool.schedule(std::bind(&Task::produce, this));
        pool.schedule(std::bind(&Task::consume, this));
        pool.wait();

    }
    void Task::produce(){

        while(!taskFinished && !buffer->isFull()){ // read until Databuffer  , read all until child send empty block
            Block res = root->read();
            buffer->push(res);
            if(!res){
                taskFinished = true;
                break;
            }
        }

    }
    std::shared_ptr<ConcurrentBoundedQueue<Block>> Task::getPartitionBuffer(size_t partitionId){

    }
    void Task::consume(){
        while(!buffer->isEmpty() || !taskFinished){
            Block block;
            buffer->pop(block);
            std::map<UInt32, Block> blocks = repartition(block);

            for(auto p : blocks){
                if(!partitionBuffer[p.first]->isFull())
                    partitionBuffer[p.first]->push(p.second);
            }
        }
    }

    void Task::execute(std::shared_ptr<ConcurrentBoundedQueue<Block>> buffer){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
            buffer->push(res);
        }
        Block end ;
        buffer->push(end);
    }
    void Task::finish(){

    }
    std::vector<std::string> Task::split1(const std::string& str, const std::string& delim) {
        std::vector<std::string>  res;
        if("" == str) return res;
        //先将要切割的字符串从string类型转换为char*类型
        char * strs = new char[str.length() + 1] ; //不要忘了
        strcpy(strs, str.c_str());

        char * d = new char[delim.length() + 1];
        strcpy(d, delim.c_str());

        char *p = strtok(strs, d);
        while(p) {
            std::string s = p; //分割得到的字符串转换为string类型
            res.push_back(s); //存入结果数组
            p = strtok(NULL, d);
        }

        return res;
    }

    /*
    void Task::receiveHashTable(Block &block ,std::string childTaskId) {

        hashTableLock.lock();
        auto stringVec = split1(childTaskId,"_");
        assert(stringVec.size() == 3);
        std::string childStageId = stringVec[0] + "_" + stringVec[1];

        bool  findJoin  = false;

        ProjectExecNode * projectExecNode = typeid_cast<ProjectExecNode*>( root.get() );
        assert(projectExecNode != NULL);
        std::shared_ptr<ExecNode> cur = projectExecNode->getChild();
        while(cur){
            JoinExecNode * joinExecNode = typeid_cast<JoinExecNode*>( cur.get() );

            if(joinExecNode && joinExecNode->getHashTableStageId() == childStageId){
                joinExecNode->getJoin()->insertFromBlock(block);
                findJoin = true;
                break;
            }
            cur = cur->getChild();
        }

        if(!findJoin){
            throw Exception("not find join node in task:" + taskId);
        }
        hashTableLock.unlock();


    }

    void Task::receiveBlock(Block &block) { // receive block and put to buffer

        buffer->push(block);// last block is empty

        LOG_DEBUG(log,"task :" + taskId + " receive block ,buffer size :" << buffer->size());

    }

    bool Task::highWaterMark() { // when receive buffer is more than 80%
        return  ( (float)(buffer->size()) / (float)(buffer->max())) > 0.8;
    }
    */
    void Task::createExecNodeByClient(){

        std::shared_ptr<DataClientExecNode> node = std::make_shared<DataClientExecNode>(client,this,mainTableStageIds)  ;
        auto  cur = root;
        auto  pre = root ;
        while(cur){
            pre = cur ;
            cur = cur->getChild();
        }
        pre->setChild(node);

    }
    void Task::debugString(std::stringstream  & ss ,size_t blankNum) {

        INSERT_BLANK(blankNum);
        ss << "task id :" << getTaskId();
        ss << "     receive from:"<< exechangeTaskDataSource.receiver.ip <<":"<<exechangeTaskDataSource.receiver.dataPort;
        ss << "     send to  :";
        for(auto pair : exechangeTaskDataDest.receiverInfo){
            int partitionId =  pair.first;
            ss << partitionId;
            ss << ",";
            DataReceiverInfo receiverInfo = pair.second;
            ss << receiverInfo.ip << ":" << receiverInfo.dataPort;
            ss << "|";

        }
        //ss << "\n";
        ss << "     distribute keys:";
        for(auto key : exechangeTaskDataDest.distributeKeys){
            ss << key ;
            ss << "#";
        }
        //ss << "\n";
        ss << "      execnode :";
        for(auto e : execNodes){

            ss <<  e->getName();
            ss << ",";
        }
        //ss << "\n";
    }





}