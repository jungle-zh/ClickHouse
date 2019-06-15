//
// Created by usser on 2019/6/15.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>

namespace DB {


class MergeExecNode  :public ExecNode {

private:

    Block inputHeader ;
    NamesAndTypesList aggregation_keys;
    NamesAndTypesList aggregated_columns;
    AggregateDescriptions  aggregate_descriptions ;

};

}



