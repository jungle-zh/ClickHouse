//
// Created by jungle on 19-6-18.
//

#ifndef CLICKHOUSE_DATACONNECTIONCLIENT_H
#define CLICKHOUSE_DATACONNECTIONCLIENT_H


class DataConnectionClient {


    BlockInputStreamPtr block_in;  //ReadBufferFromPocoSocket::poll(size_t timeout_microseconds)

    BlockOutputStreamPtr block_out; //NativeBlockOutputStream



};


#endif //CLICKHOUSE_DATACONNECTIONCLIENT_H
