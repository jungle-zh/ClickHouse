//
// Created by Administrator on 2019/5/2.
//

#include <Interpreters/PlanNode/AggPlanNode.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Interpreters/PlanNode/ExpressionActionsNode.h>
#include <Interpreters/ExecNode/AggExecNode.h>

namespace DB {



    std::shared_ptr<ExecNode> AggPlanNode::createExecNode() {
        auto aggExecNode  = std::make_shared<AggExecNode>(
                inputHeader,
                aggregationKeys,
                aggregateColumns,
                aggregateDescriptions,
                actions,
                context,
                final
                );

        return   aggExecNode ;
    }

    Block  AggPlanNode::getHeader(){

        ColumnNumbers keys;
        for (const auto & pair : aggregationKeys)
            keys.push_back(inputHeader.getPositionByName(pair.name));

        auto settings = context->getSettings();
        bool allow_to_use_two_level_group_by = false;
        bool overflow_row = false;
        auto params  = std::make_shared<Aggregator::Params>(inputHeader, keys, aggregateDescriptions,
                                                            overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                                                            settings.compile ? &context->getCompiler() : nullptr, settings.min_count_to_compile,
                                                            allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                                                            allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                                                            settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                                                            context->getTemporaryPath());

        //Aggregator(std::move(params));
        auto aggregator =  std::make_shared<Aggregator>(*params);

        return aggregator->getHeader(final, true);
    }


}
