//
// Created by usser on 2019/6/15.
//

#include <Interpreters/ExecNode/MergeExecNode.h>


namespace DB {

    Block  MergeExecNode::getHeader() const {
        return aggregator->getHeader(final);
    }


    void MergeExecNode::readPrefix() {

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
    Block MergeExecNode::readImpl(){

        if (!executed)
        {
            executed = true;
            AggregatedDataVariants data_variants;

            Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
            aggregator->setCancellationHook(hook);

            aggregator->mergeStream(children.back(), data_variants, max_threads);
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

}

