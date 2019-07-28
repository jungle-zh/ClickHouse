//
// Created by usser on 2019/6/15.
//

#include <Interpreters/ExecNode/MergeExecNode.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <Core/Field.h>

namespace DB {


    void MergeExecNode::serialize(WriteBuffer & buffer) {

        ExecNode::serializeHeader(inputHeader,buffer);
        //ExecNode::serializeExpressActions(*actions,buffer);
        aggregationKeys.writeText(buffer);
        aggregateColumns.writeText(buffer);
        serializeAggDesc(buffer);

    }

    std::shared_ptr<ExecNode> MergeExecNode::deserialize(ReadBuffer & buffer) {

        Block header =  ExecNode::deSerializeHeader(buffer);
        //std::shared_ptr<ExpressionActions> actions = ExecNode::deSerializeExpressActions(buffer);
        NamesAndTypesList aggregationKeys_ ;
        NamesAndTypesList  aggregateColumns_;
        aggregationKeys_.readText(buffer);
        aggregateColumns_.readText(buffer);
        AggregateDescriptions desc_ = deserializeAggDesc(buffer);

        Context * context = NULL;
        return std::make_shared<MergeExecNode>(header,aggregationKeys_,aggregateColumns_,desc_, context);

    }

    void MergeExecNode::serializeAggDesc(WriteBuffer & buffer){

        int descNum = aggregateDescriptions.size();
        writeVarUInt(descNum,buffer);
        for(int i=0;i<descNum ; ++i){

            writeStringBinary(aggregateDescriptions[i].function_name,buffer);
            writeVarUInt(aggregateDescriptions[i].argument_types.size(),buffer);
            for(size_t j=0;j < aggregateDescriptions[i].argument_types.size();++j){
                writeStringBinary(aggregateDescriptions[i].argument_types[i],buffer);
            }

            writeBinary(aggregateDescriptions[i].parameters,buffer);

            writeVarUInt(aggregateDescriptions[i].arguments.size(),buffer);
            for(size_t j=0; j < aggregateDescriptions[i].arguments.size(); ++j){
                writeVarUInt(aggregateDescriptions[i].arguments[j],buffer);
            }

            writeVarUInt(aggregateDescriptions[i].argument_names.size(),buffer);
            for(size_t j=0; j < aggregateDescriptions[i].argument_names.size(); ++j){
                writeStringBinary(aggregateDescriptions[i].argument_names[j],buffer);
            }
            writeStringBinary(aggregateDescriptions[i].column_name,buffer);

        }

    }

    AggregateDescriptions MergeExecNode::deserializeAggDesc(ReadBuffer & buffer){

        size_t descNum  = 0;
        readVarUInt(descNum, buffer);

        AggregateDescriptions ret;
        for(size_t i =0 ;i< descNum ;++i){
            AggregateDescription desc ;

            readStringBinary(desc.function_name,buffer);
            size_t argument_types_size ;
            readVarUInt(argument_types_size, buffer);
            for(size_t j  = 0 ; j < argument_types_size; ++j){
                std::string argument_types ;
                readStringBinary(argument_types,buffer);
                desc.argument_types.push_back(argument_types);
            }
            DB::readBinary(desc.parameters,buffer);

            DataTypes types ;
            for(size_t j =0 ; j < argument_types_size; ++j){
                DataTypePtr type = DataTypeFactory::instance().get(desc.argument_types[j]);
                types.push_back(type);
            }

            desc.function = AggregateFunctionFactory::instance().get(desc.function_name, types, desc.parameters);

            size_t argument_size ;
            readVarUInt(argument_size, buffer);
            for(size_t j  = 0 ; j < argument_size; ++j){
                size_t argument ;
                readVarUInt(argument,buffer);
                desc.arguments.push_back(argument);
            }

            size_t argument_names_size ;
            readVarUInt(argument_names_size, buffer);
            for(size_t j  = 0 ; j < argument_names_size; ++j){
                std::string argument_name ;
                readStringBinary(argument_name,buffer);
                desc.argument_names.push_back(argument_name);
            }

            readStringBinary(desc.column_name, buffer);

            ret.push_back(desc);
        }
        return  ret ;

    }


    Block  MergeExecNode::getHeader(bool isAnalyze)  {
        return aggregator->getHeader(final,isAnalyze );
    }


    void MergeExecNode::readPrefix() {

        ColumnNumbers keys;
        for (const auto & pair : aggregationKeys)
            keys.push_back(inputHeader.getPositionByName(pair.name));

        params  = std::make_unique<Aggregator::Params>(inputHeader, keys, aggregateDescriptions,
                                                       overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                                                       settings.compile ? &context->getCompiler() : nullptr, settings.min_count_to_compile,
                                                       allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                                                       allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                                                       settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                                                       context->getTemporaryPath());

        //Aggregator(std::move(params));
        aggregator =  std::make_unique<Aggregator>(*params);


    }
    Block MergeExecNode::read(){

        if (!executed)
        {
            executed = true;
            AggregatedDataVariants data_variants;

            Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
            aggregator->setCancellationHook(hook);

            aggregator->mergeStream(children, data_variants, max_threads);
            blocks = aggregator->convertToBlocks(data_variants, final, max_threads);
            it = blocks.begin();
        }

        Block res;
        if (isCancelledOrThrowIfKilled() || it == blocks.end())
            return res;

        res = std::move(*it);
        ++it;

        return res;
    }

    bool MergeExecNode::isCancelled() {
        return false;

    }
    bool MergeExecNode::isCancelledOrThrowIfKilled(){
        return false;
    }

}

