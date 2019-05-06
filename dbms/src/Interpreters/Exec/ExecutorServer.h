//
// Created by Administrator on 2019/5/6.
//

#pragma once

//receive request from executorProxy ,
// deserilize task ,task create  execute and push data to out put node
class ExecutorServer {



 std::shared_ptr<TaskDataExchangeServer> dataExchange ;

 std::vector<std::shared_ptr<Task>> activeTasks;

 //  threads move task data from  dataExchange to activeTasks inputNode
 std::vector<std::Thread>  dataTrans;


};