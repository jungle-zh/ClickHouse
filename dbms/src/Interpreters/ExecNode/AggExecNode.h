//
// Created by usser on 2019/6/15.
//
#pragma  once

#include <Interpreters/ExecNode/ExecNode.h>
#include <Interpreters/AggregateDescription.h>

namespace DB {

    class AggExecNode : public ExecNode {

    private:

        Block inputHeader ;
        NamesAndTypesList aggregation_keys;
        NamesAndTypesList aggregated_columns;
        AggregateDescriptions  aggregate_descriptions ;

        ExpressionActions actions;




    };

}


