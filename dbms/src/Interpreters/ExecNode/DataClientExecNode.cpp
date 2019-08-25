//
// Created by jungle on 19-6-30.
//


#include <common/logger_useful.h>
#include "DataClientExecNode.h"
#include <Interpreters/Task.h>
#include <Interpreters/DataExechangeClient.h>

namespace DB {




    Block DataClientExecNode::readFromBuffer() {
            assert(buffer->size());
            Block ret;
            buffer->pop(ret);
            if (!ret) {
                LOG_DEBUG(log, "task: " + task->getTaskId() + " received all child task data");
            }
            return ret;

    }
    void  DataClientExecNode::readFromRemote(){

        assert(bufferMaxSize - buffer->size() > 0 );
        for(size_t i=0;i< bufferMaxSize - buffer->size();++i){

            bool allStageFinished = true;
            for(auto stageId : stageIds){
                if(finishedStage.find(stageId) == finishedStage.end()){
                    Block block = client->read(stageId);
                    buffer->push(block);
                    if(block){
                        allStageFinished = false;
                        buffer->push(block);
                    } else{
                        finishedStage.insert(stageId);
                    }
                }
            }

            if(allStageFinished){
                Block empty;
                buffer->push(empty);
                LOG_DEBUG(log,"task :" + task->getTaskId() +" all stage data read finish" );
            }

        }

    }
    Block DataClientExecNode::read() {

        if(buffer->size()){
            return  readFromBuffer();
        } else {
            readFromRemote();
            return  readFromBuffer();
        }
    }

    Block DataClientExecNode::getHeader(bool isAnalyze) {
        (void) isAnalyze;
        if(buffer->size())
            return  buffer->front();
        else {
            readFromBuffer();
            return  buffer->front();
        }


    }

    Block DataClientExecNode::getInputHeader() {
        if(buffer->size())
            return  buffer->front();
        else {
            readFromBuffer();
            return  buffer->front();
        }
    }





}