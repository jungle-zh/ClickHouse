//
// Created by usser on 2019/6/13.
//

#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB {


    void ExecNode::serializeExpressActions(ExpressionActions &actions, WriteBuffer &buffer) {


        actions.getRequiredColumnsWithTypes().writeText(buffer);

        //Block  preActionSampleBlock ;
        //for (const auto & input_elem : actions.getRequiredColumnsWithTypes())
        //    preActionSampleBlock.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));
        //serializeHeader(preActionSampleBlock,buffer);

        size_t actionNum = actions.getActions().size();
        writeVarUInt(actionNum,buffer);
        for(const ExpressionAction & action : actions.getActions()){

            switch (action.type) {

                case ExpressionAction::ADD_COLUMN : {

                    writeVarUInt(ExpressionAction::ADD_COLUMN,buffer);
                    writeStringBinary(action.result_name,buffer);
                    writeStringBinary(action.result_type->getName(),buffer);
                    if(const ColumnConst* columnConst = static_cast< const ColumnConst *>(action.added_column.get())){ //in getActionsImpl createColumnConst

                        action.result_type->serializeBinary((*columnConst)[0],buffer); //call DataTypeNumberBase<T>::serializeBinary

                    } else {
                        throw  Exception("add column " +action.result_name + " type not support yet" );
                    }

                    break;
                }
                case ExpressionAction::REMOVE_COLUMN : {

                    writeVarUInt(ExpressionAction::REMOVE_COLUMN,buffer);
                    writeStringBinary(action.source_name,buffer);
                    break;

                }
                case ExpressionAction::COPY_COLUMN : {

                    writeVarUInt(ExpressionAction::COPY_COLUMN,buffer);
                    writeStringBinary(action.source_name,buffer);
                    writeStringBinary(action.result_name,buffer);
                    writeStringBinary(action.result_type->getName(),buffer);
                    break;
                }
                case ExpressionAction::APPLY_FUNCTION : {

                    writeVarUInt(ExpressionAction::APPLY_FUNCTION,buffer);
                    writeStringBinary(action.result_name,buffer);
                    writeStringBinary(action.result_type->getName(),buffer);
                    writeVarUInt(action.argument_names.size(),buffer);
                    for(size_t i=0 ;i< action.argument_names.size() ; ++i){
                        writeStringBinary(action.argument_names[i],buffer);
                    }

                    writeStringBinary(action.function_name,buffer);

                    break;
                }
                case ExpressionAction::PROJECT :{
                   writeVarUInt(ExpressionAction::PROJECT,buffer);
                   size_t projectNum = action.projection.size();
                   writeVarUInt(projectNum,buffer) ;
                   for(size_t i =0;i< projectNum; ++i){
                       writeStringBinary(action.projection[i].first,buffer);
                       writeStringBinary(action.projection[i].second,buffer);
                   }
                    break;
                }
                default:
                    break;
            }
        }


    }



    std::shared_ptr<ExpressionActions> ExecNode::deSerializeExpressActions( ReadBuffer &buffer ,Context * context) {


        NamesAndTypesList inputColumn;
        inputColumn.readText(buffer);


        //Settings settings;
        auto actions =  std::make_shared<ExpressionActions>(inputColumn,context->getSettingsRef());

        size_t  actionNum ;
        readVarUInt(actionNum,buffer);
        for(size_t i = 0;i< actionNum ; ++i){

            size_t  type ;
            ExpressionAction action;
            readVarUInt(type,buffer);
            switch (type) {
                case ExpressionAction::ADD_COLUMN :{

                    readStringBinary(action.result_name,buffer);
                    std::string type ;
                    readStringBinary(type,buffer);
                    action.result_type =  createDataTypeFromString(type);
                    Field field;
                    action.result_type->deserializeBinary(field,buffer);
                    try {
                        action.added_column = action.result_type->createColumnConst(1, field);
                    }catch (Exception e) {
                        throw e;
                    }
                    action.type = ExpressionAction::ADD_COLUMN;
                    break;
                }
                case ExpressionAction::REMOVE_COLUMN :{

                    readStringBinary(action.source_name,buffer);
                    action.type = ExpressionAction::REMOVE_COLUMN;
                    break;
                }
                case ExpressionAction::APPLY_FUNCTION : {

                    readStringBinary(action.result_name,buffer);
                    std::string funReturnype ;
                    readStringBinary(funReturnype,buffer);
                    // action.result_type = createDataNumTypeFromString(type);
                    size_t arg_num ;
                    readVarUInt(arg_num,buffer);
                    for(size_t i =0; i< arg_num;++i){
                        readStringBinary(action.argument_names[i],buffer);
                    }
                    readStringBinary(action.function_name,buffer);

                    //Context context = Context::createGlobal();
                    action.function_builder = FunctionFactory::instance().get(action.function_name, *context);
                    action.type = ExpressionAction::APPLY_FUNCTION;
                    //action.function will be create in ExpressionActions::addImpl

                    /*
                    ColumnsWithTypeAndName arguments(action.argument_names.size());
                    for (size_t i = 0; i < action.argument_names.size(); ++i)
                    {
                        if (!preActionsHeader.has(action.argument_names[i]))
                            throw Exception("Unknown identifier: '" + action.argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
                        arguments[i] = preActionsHeader.getByName(action.argument_names[i]);
                    }

                    action.function = action.function_builder->build(arguments);
                    action.result_type = action.function->getReturnType();
                    */
                    break;
                }
                case ExpressionAction::COPY_COLUMN : {
                    readStringBinary(action.source_name ,buffer);
                    readStringBinary(action.result_name, buffer);
                    std::string type ;
                    readStringBinary(type,buffer);
                    action.result_type = createDataTypeFromString(type);
                    action.type = ExpressionAction::COPY_COLUMN;
                    break;
                }
                case ExpressionAction::PROJECT : {
                    size_t projectNum ;
                    readVarUInt(projectNum,buffer);
                    for(size_t i=0;i<projectNum;++i){
                        std::string first ;
                        std::string second ;
                        readStringBinary(first,buffer);
                        readStringBinary(second,buffer);
                        action.projection.push_back(std::pair<std::string,std::string>(first,second));
                    }
                    action.type = ExpressionAction::PROJECT;
                    break;
                }
                default:
                    break;
            }


            actions->add(action); // will build funtion and convert sample_block

        }

        return actions;



    }

    DataTypePtr ExecNode::createDataTypeFromString(std::string type){

        const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
        return data_type_factory.get(type);
        /*
        DataTypePtr ret;
        if(type == "UInt8"){
           ret = std::make_shared<DataTypeUInt8>();
        }else if(type == "UInt16"){
            ret = std::make_shared<DataTypeUInt16>();
        }else if(type == "UInt32"){
            ret = std::make_shared<DataTypeUInt32>();
        }else if(type == "UInt64"){
            ret = std::make_shared<DataTypeUInt64>();
        }else if(type == "Int8"){
            ret = std::make_shared<DataTypeInt8>();
        }else if(type == "Int16"){
            ret = std::make_shared<DataTypeInt16>();
        } else if(type == "Int32"){
            ret = std::make_shared<DataTypeInt32 >();
        } else if(type == "Int64"){
            ret = std::make_shared<DataTypeInt64 >();
        } else if(type == "Float32"){
            ret = std::make_shared<DataTypeFloat32 >();
        } else if(type == "Float64"){
            ret = std::make_shared<DataTypeFloat64 >();
        } else if(type == "String"){
            ret = std::make_shared<DataTypeString >();
        } else if(type == "Array"){
            ret = std::make_shared<DataTypeArray >();
        } else if(type == "Tuple"){
            ret = std::make_shared<DataTypeTuple >();
        } else {
            throw  Exception("unsupport Array dataType");
        }
         return ret; 
         */



    }

    void ExecNode::serializeHeader(Block & block ,WriteBuffer & buffer){

        /*
        header.getNamesAndTypesList().writeText(buffer);
        */
        size_t columns = block.columns();
        size_t rows = block.rows();
        writeVarUInt(columns, buffer);
        writeVarUInt(rows, buffer);

        for (size_t i = 0; i < columns; ++i) {


            const ColumnWithTypeAndName &column = block.safeGetByPosition(i);

            /// Name
            writeStringBinary(column.name, buffer);

            /// Type
            String type_name = column.type->getName();



            writeStringBinary(type_name, buffer);

            /// Data
            if (rows)   {
                ColumnPtr full_column;

                if (ColumnPtr converted = column.column->convertToFullColumnIfConst())
                    full_column = converted;
                else
                    full_column = column.column;

                IDataType::OutputStreamGetter output_stream_getter = [&] (const IDataType::SubstreamPath &) { return &buffer; };
                column.type->serializeBinaryBulkWithMultipleStreams(*full_column, output_stream_getter, 0, 0, false, {});
            }
        }

    }

    Block ExecNode::deSerializeHeader( DB::ReadBuffer &buffer) {

        /*
        Block header ;
        NamesAndTypesList res ;
        res.readText(buffer);

        for (const auto & input_elem : res)
            header.insert(ColumnWithTypeAndName(nullptr, input_elem.type, input_elem.name));

        return header;
        */
        size_t columns = 0;
        size_t rows = 0;

        readVarUInt(columns, buffer);
        readVarUInt(rows, buffer);
        Block res;
        const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
        for (size_t i = 0; i < columns; ++i)
        {

            ColumnWithTypeAndName column;

            /// Name
            readBinary(column.name, buffer);

            /// Type
            String type_name;
            readBinary(type_name, buffer);
            column.type = data_type_factory.get(type_name);



            /// Data
            MutableColumnPtr read_column = column.type->createColumn();

            double avg_value_size_hint = 0;
            if (rows >0 )   {
                IDataType::InputStreamGetter input_stream_getter = [&] (const IDataType::SubstreamPath &) { return &buffer; };
                column.type->deserializeBinaryBulkWithMultipleStreams(*read_column, input_stream_getter, rows, avg_value_size_hint, false, {});

                if (column.column->size() != rows)
                    throw Exception("Cannot read all data in NativeBlockInputStream.", ErrorCodes::CANNOT_READ_ALL_DATA);
            }
               // readData(*column.type, *read_column, istr, rows, avg_value_size_hint);

            column.column = std::move(read_column);

            res.insert(std::move(column));


        }
        return  res;

    }


}