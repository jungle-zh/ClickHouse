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
        void execute();
        void finish();

        Task(DataSource source, DataDest dest, std::vector<std::shared_ptr<ExecNode>> nodes);

    private:
        std::vector<std::shared_ptr<ExecNode>> execNodes;
        std::shared_ptr<ExecNode> root;
        std::unique_ptr<DataReceiver> receiver;
        std::unique_ptr<DataSender> sender;

        Block inputHeader;
        DataSource dataSource;
        DataDest dataDest;

    };


}