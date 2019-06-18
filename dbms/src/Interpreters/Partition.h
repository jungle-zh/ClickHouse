//
// Created by jungle on 19-6-18.
//

#ifndef CLICKHOUSE_PARTITION_H
#define CLICKHOUSE_PARTITION_H



enum DataSourceType {
    table,
    exechange
};

struct DataSource {
    DataSourceType type;
};

struct Partition{
    UInt32 partitionId;
    std::string ip ;
    UInt32 port ;
    std::string taskId;
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
