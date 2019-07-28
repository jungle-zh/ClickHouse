//
// Created by usser on 2019/6/13.
//
#pragma  once

#include <vector>
#include <Interpreters/ExpressionActions.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB {



class ExecNode  {



    //virtual void serialize(WriteBuffer & buffer);

    //virtual std::shared_ptr<ExecNode> deserialize(ReadBuffer & buffer);

public:
    enum NodeType {

        TAgg,
        TJoin,
        TFilter,
        TScan
    };

    ExecNode(){};
    virtual  ~ExecNode(){};

    virtual Block read() = 0;
    virtual void  readPrefix() = 0 ;
    virtual void  readSuffix() = 0;
    virtual Block getHeader (bool isAnalyze) = 0;
    virtual Block getInputHeader() = 0;
    static void serializeExpressActions( ExpressionActions & actions,WriteBuffer & buffer );

    static std::shared_ptr<ExpressionActions> deSerializeExpressActions( ReadBuffer & buffer,Context * context);

    static void serializeHeader(Block & header ,WriteBuffer & buffer);

    static Block deSerializeHeader(ReadBuffer & buffer);


    static DataTypePtr createDataTypeFromString(std::string type);



    std::shared_ptr<ExecNode> children;

    std::shared_ptr<ExecNode> getChild() {
        return  children;
    }
    void setChild(std::shared_ptr<ExecNode>  c){
        children = c;
    }

   // Settings settings;
   // Context  * context;

};


}