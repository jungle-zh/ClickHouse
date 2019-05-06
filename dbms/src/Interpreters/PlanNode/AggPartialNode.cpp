//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/AggPartialNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/PlanNode/ExpressionActionsNode.h>
namespace DB {
void  AggPartialNode::init(){


    beforeAggExpression();
}
void  AggPartialNode::beforeAggExpression(){


    std::shared_ptr<ExpressionActionsNode> expNode = std::make_shared<ExpressionActionsNode>(expressionActions);

    PlanNodePtr child =  getUnaryChild();
    expNode->setUnaryChild(child);
    this->setUnaryChild(expNode);


}
Block AggPartialNode::read() {



    LOG_DEBUG(&Logger::get("AggregatingBlockInputStream"),"start readImpl");
    if (!executed)
    {
        executed = true;
        AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        aggregator.execute(getUnaryChild(), *data_variants);

        if (!aggregator.hasTemporaryFiles())
        {
            ManyAggregatedDataVariants many_data { data_variants };
            impl = aggregator.mergeAndConvertToBlocks(many_data, final, 1);
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
              *  then read and merge them, spending the minimum amount of memory.
              */

            ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

            if (!isCancelled())
            {
                /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
                if (data_variants->size())
                    aggregator.writeToTemporaryFile(*data_variants);
            }

            const auto & files = aggregator.getTemporaryFiles();
            BlockInputStreams input_streams;
            for (const auto & file : files.files)
            {
                temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path()));
                input_streams.emplace_back(temporary_inputs.back()->block_in);
            }

            LOG_TRACE(log, "Will merge " << files.files.size() << " temporary files of size "
                                         << (files.sum_size_compressed / 1048576.0) << " MiB compressed, "
                                         << (files.sum_size_uncompressed / 1048576.0) << " MiB uncompressed.");

            impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(input_streams, params, final, 1, 1);
        }
    }

    if (isCancelledOrThrowIfKilled() || !impl)
        return {};

    return impl->read();


}



}
