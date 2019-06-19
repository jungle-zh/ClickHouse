//
// Created by jungle on 19-6-18.
//

#ifndef CLICKHOUSE_PARTITION_H
#define CLICKHOUSE_PARTITION_H



enum DataSourceType {
    table,
    exechange
};

struct ServerNode {
    std::string ip;
    UInt32 port;
};
struct TaskReceiver {

    TaskReceiver(std::string ip_,UInt32 port_){
        ip = ip_;
        taskPort = port_;

    }
    std::string ip;
    UInt32 taskPort;
};
struct Executor {
    std::string ip;
    UInt32 dataPort;
    std::string taskId;
};
struct Partition{
    UInt32 partitionId;
    Executor executor;
    std::string taskId;
};


struct DataSource {
    DataSourceType type;
    std::vector<std::string> distributeKeys;
    Partition partition;
};
struct DataDest {
    std::vector<std::string> distributeKeys;
    std::vector<Partition> partitions;
};


struct Distribution {

    Distribution(std::vector<std::string> keys_,int partitionNum_):distributeKeys(keys_),partitionNum(partitionNum_){};
    Distribution(){};

    std::vector<std::string> distributeKeys;


    int partitionNum ;

    std::vector<Partition> partitions;

    bool isPartitionAssigned = false;

    void setPartitions( std::vector<Partition> part){ partitions = part;isPartitionAssigned = true;}

    bool equals(std::shared_ptr<Distribution> right); // paritionNum and distributeKeys all equal
    bool keyEquals(std::vector<std::string> keys);

};

#endif //CLICKHOUSE_PARTITION_H
