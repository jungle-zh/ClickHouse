//
// Created by usser on 2019/6/15.
//
#pragma  once

#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <Interpreters/PlanNode/AggPlanNode.h>

namespace DB {



    class AggExecNode : public ExecNode  {

        struct TemporaryFileStream
        {
            ReadBufferFromFile file_in;
            CompressedReadBuffer compressed_in;
            BlockInputStreamPtr block_in;

            TemporaryFileStream(const std::string & path);
        };

    public:

        virtual ~AggExecNode() {}
        void   serialize(WriteBuffer & buffer) ;
        static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;
        void  serializeAggDesc(WriteBuffer & buffer);
        static AggregateDescriptions deserializeAggDesc (ReadBuffer & buffer);
        void  readPrefix() override;
        void  readSuffix() override;
        Block read() override ;
        Block getHeader ()  override;
        Block getInputHeader() override;
        bool isCancelledOrThrowIfKilled() { return false;}
        bool isCancelled(){ return false;}

        AggExecNode();



        AggExecNode(Block & inputHeader_ , NamesAndTypesList & aggkeys_ ,  NamesAndTypesList & aggColumn_,
                    AggregateDescriptions & desc_ ,ExpressionActionsPtr & actions_ ,Settings & settings_,Context * context_ ):
                    inputHeader(inputHeader_),
                    aggregationKeys(aggkeys_),
                    aggregateColumns(aggColumn_),
                    aggregateDescriptions(desc_),
                    actions(actions_),
                    settings(settings_),
                    context(context_)
                    {
                    }
        AggExecNode(AggPlanNode * planNode);


    private:

        Block inputHeader ;
        //Block HeaderAfterActions;
        NamesAndTypesList aggregationKeys;
        NamesAndTypesList aggregateColumns;
        AggregateDescriptions  aggregateDescriptions ;


        bool allow_to_use_two_level_group_by;
        bool overflow_row;
        bool executed;
        ExpressionActionsPtr actions;
        //ExpressionActions actions;

        bool  final;

        std::unique_ptr<Aggregator::Params> params;
        std::unique_ptr<Aggregator> aggregator;
        std::unique_ptr<IBlockInputStream> impl;

        Settings  settings  ;


        Context * context;
        std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;
        Logger * log = &Logger::get("AggExecNode");

    };

}


