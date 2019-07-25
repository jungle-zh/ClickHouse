//
// Created by jungle on 19-6-18.
//

#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/ExecNode/AggExecNode.h>
#include <Common/typeid_cast.h>
#include <Interpreters/ExecNode/FilterExecNode.h>
#include <Interpreters/ExecNode/JoinExecNode.h>
#include <Interpreters/ExecNode/MergeExecNode.h>
#include <Interpreters/ExecNode/ProjectExecNode.h>
#include <Interpreters/ExecNode/ScanExecNode.h>
#include <Interpreters/ExecNode/UnionExecNode.h>
#include "TaskOutputStream.h"

namespace DB {

    void TaskOutputStream::init() {

       // out = std::make_shared<WriteBufferFromPocoSocket>(*socket);

    }

    void TaskOutputStream::write(Task  &  task) {


        //task.getExecSources()
        ExechangeTaskDataSource exechangeSource  = task.getExecSources();
        ExechangeTaskDataDest dest =  task.getExecDest();
        ScanTaskDataSource  scanSource =  task.getScanSource();

        write(exechangeSource);
        write(dest);
        write(scanSource);
        write(task.getExecNodes());



    }

    void  TaskOutputStream::write(std::shared_ptr<ExecNode> e) {

        if(AggExecNode * aggExecNode = typeid_cast<AggExecNode *>(e.get())){
            aggExecNode->serialize(*out);
        }else if(FilterExecNode * filterExecNode = typeid_cast<FilterExecNode *>(e.get())){
            filterExecNode->serialize(*out);
        }else if(JoinExecNode * joinExecNode = typeid_cast<JoinExecNode *>(e.get())){
            joinExecNode->serialize(*out);
        }else if(MergeExecNode * mergeExecNode = typeid_cast<MergeExecNode *>(e.get())){
            mergeExecNode->serialize(*out);
        }else if(ProjectExecNode * projectExecNode = typeid_cast<ProjectExecNode *>(e.get())){
            projectExecNode->serialize(*out);
        }else if(ScanExecNode * scanExecNode = typeid_cast<ScanExecNode *>(e.get())){
            scanExecNode->serialize(*out);
        }else if(UnionExecNode * unionExecNode = typeid_cast<UnionExecNode *>(e.get())){
            unionExecNode->serialize(*out);
        }


    }
    void  TaskOutputStream::write(std::vector<std::shared_ptr<ExecNode>> execnodes) {



    }

    void  TaskOutputStream::write(ExechangeTaskDataSource & source){

    }
    void  TaskOutputStream::write(ExechangeTaskDataDest & dest){

    }
    void  TaskOutputStream::write(ScanTaskDataSource & source){

    }




}