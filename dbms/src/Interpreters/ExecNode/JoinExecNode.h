//
// Created by usser on 2019/6/15.
//

#pragma once

#include <Interpreters/ExecNode/ExecNode.h>
#include <common/logger_useful.h>
#include <Interpreters/Join.h>

namespace DB {



class JoinExecNode : public ExecNode {


public:

    std::string getName() override { return  "joinExecNode";}
    void  readPrefix() override {}
    void  readSuffix() override {};
    Block read() override ;
    Block getHeader (bool isAnalyze) override;
    Block getInputHeader() override { return  getHeader(true);};

    virtual ~JoinExecNode() {}
    JoinExecNode(Names  & joinKey_ , Block & mainTableHeader_ , Block & hashTableHeader_,
                 std::string joinKind_,std::string strictness_,Block adjustHeader_, std::string hashTableStageId_):
                 joinKey(joinKey_),
                 mainTableHeader(mainTableHeader_),
                 hashTableHeader(hashTableHeader_),
                 joinKind(joinKind_),
                 strictness(strictness_),
                 adjustHeader(adjustHeader_),
                 hashTableStageId(hashTableStageId_){

        if(joinKind == "Comma"){
            kind =  ASTTableJoin::Kind::Comma ;
        } else if(joinKind == "Cross") {
            kind =  ASTTableJoin::Kind::Cross ;
        } else if(joinKind == "Full") {
            kind =  ASTTableJoin::Kind::Full ;
        } else if(joinKind == "Inner") {
            kind =  ASTTableJoin::Kind::Inner ;
        } else if(joinKind == "Left") {
            kind =  ASTTableJoin::Kind::Left ;
        } else if(joinKind == "Right") {
            kind =  ASTTableJoin::Kind::Right ;
        }

        if(strictness == "All"){
            strict = ASTTableJoin::Strictness::All;
        } else if(strictness == "Any"){
            strict = ASTTableJoin::Strictness::Any;
        } else {
            strict = ASTTableJoin::Strictness::Unspecified;
        }

        //Block resultHeader = getHeader(false);

        join = std::make_unique<Join>(
                joinKey, joinKey,
                settings.join_use_nulls, SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
                kind, strict,adjustHeader);


        join->setSampleBlock(hashTableHeader);

         log = &Logger::get("JoinExecNode");

    }

    void   serialize(WriteBuffer & buffer) ;
    static  std::shared_ptr<ExecNode>  deserialize(ReadBuffer & buffer) ;


public:

    Join * getJoin(){ return  join.get();}
    std::string getHashTableStageId() { return  hashTableStageId ;}
private:
    Names joinKey;
    Block mainTableHeader;
    Block hashTableHeader;
    std::string joinKind;
    std::string  strictness;

    std::unique_ptr<Join>  join;

    Settings settings ;

    ASTTableJoin::Kind kind;
    ASTTableJoin::Strictness strict;

    Block adjustHeader;
    std::string hashTableStageId;
    Poco::Logger *log;

   // std::string hashTable ;


};



}