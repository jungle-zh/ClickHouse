//
// Created by admin on 19/1/18.
//

#include <Interpreters/ExecNode/ExecNode.h>
#include <DataStreams/ExecNodeBlockInputStream.h>


namespace  DB {



BlockInputStreamPtr  ExecNode::buildChildStream(ExecNodePtr & node ){


    return  std::make_shared<ExecNodeBlockInputStream>(node );

}
std::vector<BlockInputStreamPtr> ExecNode::buildChildStreams (std::vector<ExecNodePtr> & nodes ){


    std::vector<BlockInputStreamPtr> res ;
    for( ExecNodePtr & node : nodes){

        res.push_back(buildChildStream(node));

    }

    return  res;

}



}