//
// Created by usser on 2019/6/15.
//

#include <Interpreters/ExecNode/AggExecNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Interpreters/ExecNode/ExpressionExecNode.h>

#include <DataTypes/DataTypeFactory.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <DataStreams/NativeBlockInputStream.h>

namespace ProfileEvents
{
    extern const Event ExternalNodeAggregationMerge;
}
namespace DB {


    void AggExecNode::serialize(WriteBuffer & buffer) {

        ExecNode::serializeHeader(inputHeader,buffer);
        ExecNode::serializeExpressActions(*actions,buffer);
        aggregationKeys.writeText(buffer);
        aggregateColumns.writeText(buffer);
        serializeAggDesc(buffer);
        if(final){
            writeStringBinary("final",buffer);
        } else{
            writeStringBinary("notFinal",buffer);
        }

    }

    std::shared_ptr<ExecNode> AggExecNode::deserialize(ReadBuffer & buffer,Context * context) {

        Block header =  ExecNode::deSerializeHeader(buffer);
        std::shared_ptr<ExpressionActions> actions = ExecNode::deSerializeExpressActions(buffer,context );
        NamesAndTypesList aggregationKeys_ ;
        NamesAndTypesList  aggregateColumns_;
        aggregationKeys_.readText(buffer);
        aggregateColumns_.readText(buffer);
        AggregateDescriptions desc_ = deserializeAggDesc(buffer);
        std::string isFinal;
        bool final ;
        readStringBinary(isFinal,buffer);
        if(isFinal == "final"){
            final = true;
        } else {
            final = false;
        }
        return std::make_shared<AggExecNode>(header,aggregationKeys_,aggregateColumns_,desc_,actions, context,final);

    }

    void AggExecNode::serializeAggDesc(WriteBuffer & buffer){

        size_t descNum = aggregateDescriptions.size();
        writeVarUInt(descNum,buffer);
        for(size_t i=0;i<descNum ; ++i){

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

    AggregateDescriptions AggExecNode::deserializeAggDesc(ReadBuffer & buffer){

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
            readBinary(desc.parameters,buffer);

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



    void AggExecNode::readPrefix(std::shared_ptr<DataExechangeClient> client) {
        (void) client;
        if(actions){
            actions->execute(inputHeader);

            //auto actionsPtr = std::shared_ptr<ExpressionActions>(&actions);

            auto expressionNode  =  std::make_shared<ExpressionExecNode>(actions);
            expressionNode->setChild(children);
            children = expressionNode;
        }


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

    Block AggExecNode::read() {

        if (!executed)
        {
            executed = true;
            AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

            Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
            aggregator->setCancellationHook(hook);

            aggregator->execute(children, *data_variants);

            if (!aggregator->hasTemporaryFiles())
            {
                ManyAggregatedDataVariants many_data { data_variants };
                impl = aggregator->mergeAndConvertToBlocks(many_data, final, 1);
            }
            else
            {
                /** If there are temporary files with partially-aggregated data on the disk,
                  *  then read and merge them, spending the minimum amount of memory.
                  */

               // ProfileEvents::increment(ProfileEvents::ExternalNodeAggregationMerge);

                if (!isCancelled())
                {
                    /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
                    if (data_variants->size())
                        aggregator->writeToTemporaryFile(*data_variants);
                }

                const auto & files = aggregator->getTemporaryFiles();
                BlockInputStreams input_streams;
                for (const auto & file : files.files)
                {
                    temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path()));
                    input_streams.emplace_back(temporary_inputs.back()->block_in);
                }

                LOG_TRACE(log, "Will merge " << files.files.size() << " temporary files of size "
                                             << (files.sum_size_compressed / 1048576.0) << " MiB compressed, "
                                             << (files.sum_size_uncompressed / 1048576.0) << " MiB uncompressed.");

                impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(input_streams, *params, final, 1, 1);
            }
        }

        if (isCancelledOrThrowIfKilled() || !impl)
            return {};

        return impl->read();


    }

    Block  AggExecNode::getHeader(bool isAnalyze)  {
        return aggregator->getHeader(final, isAnalyze);
    }


    AggExecNode::TemporaryFileStream::TemporaryFileStream(const std::string & path)
            : file_in(path), compressed_in(file_in),
              block_in(std::make_shared<NativeBlockInputStream>(compressed_in, 1)) {}

}






