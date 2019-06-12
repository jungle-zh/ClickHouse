//
// Created by Administrator on 2019/5/3.
//
#include <Interpreters/PlanNode/AggMergeNode.h>

namespace DB {


    Block AggMergeNode::read() {
        if (!executed)
        {
            executed = true;
            AggregatedDataVariants data_variants;

            Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
            aggregator.setCancellationHook(hook);

            aggregator.mergeStream(getUnaryChild(), data_variants, max_threads);
            blocks = aggregator.convertToBlocks(data_variants, final, max_threads);
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