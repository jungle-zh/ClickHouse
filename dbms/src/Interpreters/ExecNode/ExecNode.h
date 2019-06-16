//
// Created by usser on 2019/6/13.
//
#pragma  once

#include <vector>
#include <Interpreters/ExpressionActions.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB {



class ExecNode : public IProfilingBlockInputStream {



    virtual void serialize();

    virtual void deserialize();

    static void serializeExpressActions( ExpressionActions & actions,WriteBuffer & buffer );

    static void deSerializeExpressActions(ExpressionActions & actions , ReadBuffer & buffer);

    static void serializeHeader(Block & header ,WriteBuffer & buffer);

    static Block deSerializeHeader(ReadBuffer & buffer);


    static DataTypePtr createDataTypeFromString(std::string type);

};


}