//
// Created by jungle on 19-6-16.
//

#include "ScanExecNode.h"
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Storages/StorageTinyLog.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/typeid_cast.h>


namespace DB {


    void ScanExecNode::readPrefix(std::shared_ptr<DataExechangeClient> client ) {
        (void)(client);
        storage = context->getTable(database_name, table_name);
        //storage->getSampleBlock();
        QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
        size_t max_block_size = context->getSettings().max_block_size;
        size_t max_streams = 1;

        SelectQueryInfo info ;
        //required_columns = storage->getColumns().getNamesOfPhysical();

        const Settings & settings = context->getSettingsRef();

        char * begin = query.data();
        char * end  = query.data() + query.size();
        ParserQuery parser(end);
        ASTPtr ast;

        /// Don't limit the size of internal queries.
        size_t max_query_size = 0;

        max_query_size = settings.max_query_size;

        try
        {
            /// TODO Parser should fail early when max_query_size limit is reached.
            ast = parseQuery(parser, begin, end, "", max_query_size);
            ASTSelectWithUnionQuery *unionQuery = typeid_cast<ASTSelectWithUnionQuery *>(ast.get());
            info.query = unionQuery->list_of_selects->children[0];
            /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
            if (!(begin <= ast->range.first && ast->range.second <= end))
                throw Exception("Unexpected behavior: AST chars range is not inside source range", ErrorCodes::LOGICAL_ERROR);

        }
        catch (...)
        {
            throw;
        }
        //required_columns = storage->getColumns().getNamesOfPhysical();

        streams = storage->read(required_columns, info, *context, from_stage, max_block_size, max_streams);
    }

    Block ScanExecNode::read() {

        Block block = streams[streamIndex]->read();
        while (!block){
            streamIndex ++;
            if(streamIndex == streams.size())
                return block;
            else
                block =  streams[streamIndex]->read();
        }
        return block;
    }
    std::shared_ptr<ScanExecNode> ScanExecNode::deseralize(DB::ReadBuffer &buffer,Context * context) {
        std::string database_name ;
        std::string table_name ;
        Names required_columns;
        size_t required_columns_size;
        readStringBinary(database_name,buffer);
        readStringBinary(table_name,buffer);
        readVarUInt(required_columns_size,buffer);
        for(size_t i=0;i<required_columns_size;++i){
            std::string requird_column;
            readStringBinary(requird_column,buffer);
            required_columns.push_back(requird_column);
        }
        std::string query ;
        readStringBinary(query,buffer);

        auto ret = std::make_shared<ScanExecNode>(database_name,table_name,required_columns,query,context);
        return  ret;
    }
    void ScanExecNode::serialize(DB::WriteBuffer &buffer) {
        writeStringBinary(database_name,buffer);
        writeStringBinary(table_name,buffer);
        writeVarUInt(required_columns.size(),buffer);
        for(size_t  i=0;i<required_columns.size();++i){
            writeStringBinary(required_columns[i],buffer);
        }
        writeStringBinary(query,buffer);


    }
}