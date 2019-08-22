//
// Created by usser on 2019/6/17.
//


#include <Interpreters/DataReceiver.h>
#include <Interpreters/DataSender.h>
#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/TaskReceiverExecNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include "Task.h"
#include "Stage.h"

namespace DB {


    Task::Task(std::string taskId_, Context *context_) {
        taskId = taskId_;
        log = &Logger::get("Task");
        context = context_;
        buffer = std::make_shared<ConcurrentBoundedQueue<Block>> ();
    };
    Task::Task(DB::ExechangeTaskDataSource source, DB::ScanTaskDataSource source1, DB::ExechangeTaskDataDest dest,
               std::vector<std::shared_ptr<DB::ExecNode>> nodes, std::string taskId_,Context * context_) {

        exechangeTaskDataSource = source;
        scanTaskDataSource = source1;
        exechangeTaskDataDest = dest;
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
        buffer = std::make_shared<ConcurrentBoundedQueue<Block>> ();
    }

    void Task::setExechangeDest(ExechangeTaskDataDest & dest ) {
        exechangeTaskDataDest = dest ;

    }
    void Task::setScanSource(ScanTaskDataSource & source){
        scanTaskDataSource = source ;

    }
    void Task::setExechangeSource(ExechangeTaskDataSource & source){

        exechangeTaskDataSource = source ;
        //exechangeTaskDataSources.insert({source.childStageId,source});
        //childStageIds.push_back(source.childStageId);
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



        if(!isResultTask()){
            sender = std::make_shared<DataSender>(exechangeTaskDataDest,this,context);
            LOG_DEBUG(log,"task :" + taskId + " is not result task ,and connect to father task ");
            sender->tryConnect();    //jungle comment : block until success, create dest partion number connection ,shuffle result block using dest destribution key and send
        }
        if(exechangeTaskDataSource.inputTaskIds.size() > 0){
            receiver = std::make_shared<DataReceiver>(exechangeTaskDataSource,this,context); // will create tcp server and accept connection
            receiver->init();  // if has join then call receiveHashTable until read all  to HashTable
            createBottomExecNodeByBuffer();
        }
        LOG_DEBUG(log,"task :" + taskId + " start to receive data and execute ");

        auto curNode = root;
        while(curNode){
            curNode->readPrefix();
            curNode = curNode->getChild();
        }


    }



    std::shared_ptr<DataReceiver> findHashTableReceiver(){
        return  std::shared_ptr<DataReceiver>();
    }


    void Task::execute(){

        while(Block res = root->read()){ // read until Databuffer  , read all until child send empty block
             sender->send(res);           // logic thread  may be block in sendData if upstream buffer rich high waiter mark
        }
        Block end ;
        sender->send(end); // maybe two task send two empty block;
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

    void Task::createBottomExecNodeByBuffer(){

        std::shared_ptr<TaskReceiverExecNode> node = std::make_shared<TaskReceiverExecNode>(buffer,this)  ;
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