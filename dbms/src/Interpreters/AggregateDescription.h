#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    ColumnNumbers arguments;  // column index in block
    Names argument_names;    /// used if no `arguments` are specified.
    String column_name;      /// What name to use for a column with aggregate function values

public:
    std::string to_string(){
        std::string args = "";
        for(auto & arg : argument_names){
            args += arg;
            args += ",";
        }
        std::string res;
        res += ("function name is " + function->getName() );
        res += (", argument_names : " + args);
        return  res;
    }
};

using AggregateDescriptions = std::vector<AggregateDescription>;

}
