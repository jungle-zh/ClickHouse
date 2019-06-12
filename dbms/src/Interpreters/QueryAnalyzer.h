//
// Created by jungle on 19-6-10.
//

#pragma  once


#include <Interpreters/PlanNode/PlanNode.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB {

    //select query ast to planNode
    class QueryAnalyzer {

        public:

        std::shared_ptr<PlanNode> analyse( ASTSelectQuery * query);

    };

}
