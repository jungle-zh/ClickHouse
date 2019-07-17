//
// Created by usser on 2019/6/15.
//

#include <Interpreters/ExecNode/AggExecNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

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


    }

    void AggExecNode::serializeAggDesc(WriteBuffer & buffer){

        int descNum = aggregateDescriptions.size();
        writeVarInt(descNum,buffer);
        for(int i=0;i<descNum ; ++i){

        }

    }

    AggregateDescriptions AggExecNode::deserializeAggDesc(ReadBuffer & buffer){



    }

    std::shared_ptr<ExecNode> AggExecNode::deserialize(ReadBuffer & buffer) {

        Block header =  ExecNode::deSerializeHeader(buffer);
        ExpressionActions actions = ExecNode::deSerializeExpressActions(buffer);
        NamesAndTypesList aggregationKeys_ ;
        NamesAndTypesList  aggregateColumns_;
        aggregationKeys_.readText(buffer);
        aggregateColumns_.readText(buffer);
        AggregateDescriptions desc_ = deserializeAggDesc(buffer);

        return std::make_shared<AggExecNode>(header,aggregationKeys_,aggregateColumns_,desc_,actions, NULL);


    }

    void AggExecNode::readPrefix() {

        if(actions){
            actions.execute(inputHeader);
            assert(children.size() == 1);
            auto actionsPtr = std::shared_ptr<ExpressionActions>(&actions);
            auto expressionStream =  std::make_shared<ExpressionBlockInputStream>(children.back(),actionsPtr);
            children[0] = expressionStream;
        }


        ColumnNumbers keys;
        for (const auto & pair : aggregationKeys)
            keys.push_back(inputHeader.getPositionByName(pair.name));

        params  = std::make_unique<Aggregator::Params>(inputHeader, keys, aggregateDescriptions,
                                  overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                                  settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
                                  allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                                  allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                                  settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                                  context.getTemporaryPath());

        //Aggregator(std::move(params));
        aggregator =  std::make_unique<Aggregator>(params);


    }

    Block AggExecNode::readImpl() {

        if (!executed)
        {
            executed = true;
            AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

            Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
            aggregator->setCancellationHook(hook);

            aggregator->execute(children.back(), *data_variants);

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

                ProfileEvents::increment(ProfileEvents::ExternalNodeAggregationMerge);

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

    Block  AggExecNode::getHeader() const {
        return aggregator->getHeader(final);
    }





}
