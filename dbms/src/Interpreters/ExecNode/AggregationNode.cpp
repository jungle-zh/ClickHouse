//
// Created by admin on 19/1/20.
//

#include <Interpreters/ExecNode/AggregationNode.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Aggregator.h>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/Expr/ExprActions.h>
#include <Common/typeid_cast.h>
#include <Interpreters/Expr/FunctionExpr.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ExprActionBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/MergingAggregatedBlockInputStream.h>


namespace DB {

Block AggregationNode::getHeaderBeforeAgg() {


    //for(int i=0;i< childs.size();++i){
    //}
    actions->initWithHeader(childs[0]->getHeader()); //input column  the joined header ,finally ask exechange node

    for(size_t i=0;i< param.groupExprs.size();++i ){
        param.groupExprs[i].getExprActions(*actions);  // get the acitonPlan
    }

    for(size_t i=0;i< param.aggExprs.size();++i ){
        param.aggExprs[i].getExprActions(*actions);  // get the acitonPlan
    }

    return actions->getSampleBlock();

}


void AggregationNode::getAggregateInfoFromParam(Names & key_names,AggregateDescriptions & descriptions) {

    for(size_t i=0;i<param.groupExprs.size();++i){
        key_names.push_back(param.groupExprs[i].getName());
    }


    for(size_t i=0;i< param.aggExprs.size(); ++i){

        AggregateDescription aggregate;
        aggregate.column_name =  param.aggExprs[i].getName();
        for(size_t  j = 0 ; j < param.aggExprs[i].getChilds().size() ; ++j){
            aggregate.argument_names[j] = (param.aggExprs[i].getChilds())[j]->getName(); // child is expr tree
        }
        DataTypes types ;

        ExprActions tmp = ExprActions(childs[0]->getHeader());
        for(size_t  j = 0 ; j < param.aggExprs[i].getChilds().size() ; ++j){

            (param.aggExprs[i].getChilds())[j]->getExprActions(tmp);
            types.push_back(tmp.getSampleBlock().getByName((param.aggExprs[i].getChilds())[j]->getName()).type);

        }

        if( FunctionExpr * expr  =  typeid_cast<FunctionExpr *>(&param.aggExprs[i]) ){
            for( Field & field : (*expr->getParams())){
                aggregate.parameters.push_back(field);
            }
        } else{
            aggregate.parameters =  Array();
        }


        aggregate.function = AggregateFunctionFactory::instance().get(aggregate.column_name, types, aggregate.parameters);
        descriptions.push_back(aggregate);

    }


}


    void AggregationNode::getMergeInfoFromParam(Names & key_names,AggregateDescriptions & descriptions) {

        for(size_t i=0;i<param.groupExprs.size();++i){
            key_names.push_back(param.groupExprs[i].getName());
        }


        for(size_t i=0;i< param.aggExprs.size(); ++i){

            AggregateDescription aggregate;
            aggregate.column_name =  param.aggExprs[i].getName();
            for(size_t  j = 0 ; j < param.aggExprs[i].getChilds().size() ; ++j){
                aggregate.argument_names[j] = (param.aggExprs[i].getChilds())[j]->getName(); // child is expr tree
            }
            DataTypes types ;

            ExprActions tmp = ExprActions(childs[0]->getHeader());
            for(size_t  j = 0 ; j < param.aggExprs[i].getChilds().size() ; ++j){

               (param.aggExprs[i].getChilds())[j]->getExprActions(tmp);
               types.push_back(tmp.getSampleBlock().getByName((param.aggExprs[i].getChilds())[j]->getName()).type);

            }

            if( FunctionExpr * expr  =  typeid_cast<FunctionExpr *>(&param.aggExprs[i]) ){
                for( Field & field : (*expr->getParams())){
                    aggregate.parameters.push_back(field);
                }
            } else{
                aggregate.parameters =  Array();
            }


            aggregate.function = AggregateFunctionFactory::instance().get(aggregate.column_name, types, aggregate.parameters);
            descriptions.push_back(aggregate);

        }


    }

void AggregationNode::prepare() {
    if(final){
        prepareFinal();
    } else{
        prepareWithMergeable();
    }
}

void AggregationNode::prepareFinal() {


    Block headerBeforeMerge = childs[0]->getHeader();

    AggregateDescriptions  aggregates ;

    Names key_names;
    getMergeInfoFromParam(key_names,aggregates);
    ColumnNumbers keys;
    for (const auto & name : key_names)
        keys.push_back(headerBeforeMerge.getPositionByName(name));
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(headerBeforeMerge.getPositionByName(name));


    Aggregator::Params params(headerBeforeMerge, keys, aggregates, overflow_row);

    const Settings & settings = context.getSettingsRef();

    if (!settings.distributed_aggregation_memory_efficient && childs.size() == 1)
    {
        /// We union several sources into one, parallelizing the work.
        //executeUnion(pipeline);
        /// Now merge the aggregated blocks
        finalStream = std::make_shared<MergingAggregatedBlockInputStream>(ExecNode::buildChildStream(childs[0]), params, final, settings.max_threads);
    }
    else
    {
        finalStream = std::make_shared<MergingAggregatedMemoryEfficientBlockInputStream>(buildChildStreams(childs), params, final,
                                                                                                    max_streams,
                                                                                                    settings.aggregation_memory_efficient_merge_threads
                                                                                                    ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                                                                                                    : static_cast<size_t>(settings.max_threads));

    }

}
void AggregationNode::prepareWithMergeable() {

    for(int i=0;i< childs.size(); ++i){
        childs[i]->prepare();
    }


    Block headerBeforeAgg = getHeaderBeforeAgg();

    Names key_names;
    AggregateDescriptions aggregates;

    // query_analyzer->getAggregateInfo(key_names, aggregates);
    getAggregateInfoFromParam(key_names, aggregates);
    // Block header = pipeline.firstStream()->getHeader();


    ColumnNumbers keys;
    for (const auto & name : key_names)
        keys.push_back(headerBeforeAgg.getPositionByName(name));
    for (auto & descr : aggregates)
        if (descr.arguments.empty())
            for (const auto & name : descr.argument_names)
                descr.arguments.push_back(headerBeforeAgg.getPositionByName(name));

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    // bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;

    bool  allow_to_use_two_level_group_by = settings.max_bytes_before_external_group_by != 0;
    Aggregator::Params params(headerBeforeAgg, keys, aggregates,
                              overflow_row, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
                              settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
                              allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
                              settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set,
                              context.getTemporaryPath());

    // expr execute input stream


    std::vector<BlockInputStreamPtr>  exprStreams ; //child has the same header
    for(int i=0;i< childs.size(); ++i){
        exprStreams.push_back(std::make_shared<ExprActionBlockInputStream>(ExecNode::buildChildStream(childs[i]),actions));
    }

    if(childs.size() == 1 ){
        aggStream = std::make_shared<AggregatingBlockInputStream>(exprStreams[0], params, final);
    } else{

        aggStream = std::make_shared<ParallelAggregatingBlockInputStream>(
                exprStreams, stream_with_non_joined_data, params, final,
                max_streams,
                settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads));
    }

}



//
Block AggregationNode::getNextWithMergeable() {

    return  aggStream->read();

}

Block AggregationNode::getNextFinal() {

    return  finalStream->read();
}


}