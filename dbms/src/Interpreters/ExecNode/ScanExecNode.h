//
// Created by jungle on 19-6-16.
//

#pragma once

#include "ExecNode.h"

namespace DB {

    class IStorage;
    class Context ;
    class IAST;
    class ScanExecNode : public ExecNode {

    public:

        void serialize(WriteBuffer & buffer);
        static  std::shared_ptr<ScanExecNode> deseralize(ReadBuffer & buffer,Context * context);

        ScanExecNode( std::string database_name_ ,std::string table_name_ ,Names required_columns_,std::string query_,Context * context_) {

            database_name = database_name_;
            table_name = table_name_;
            required_columns = required_columns_;
            query = query_;
            context = context_;
            streamIndex = 0;
        }

        std::string getName() override { return  "scanExecNode";}
        Block read() override;
        void readPrefix(std::shared_ptr<DataExechangeClient>) override;
         void  readSuffix() override {};
         Block getHeader (bool isAnalyze)  override { (void)isAnalyze ;return  read() ;};
         Block getInputHeader()  override { return  Block();};

         std::string database_name;
         std::string table_name;
        Names required_columns;
            Context * context ;
        std::shared_ptr<IStorage>  storage;
        std::vector<BlockInputStreamPtr> streams;
        std::string query ;
        size_t streamIndex;
        std::shared_ptr<IAST> ast;
    };

}



