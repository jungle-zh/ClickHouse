//
// Created by Administrator on 2019/3/31.
//
#pragma once

#include <vector>
#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Core/Block.h>
#include <Interpreters/Partition.h>


namespace DB {

using  executorId =  int;
class  ExecNode ;
class  PlanNode {

public:



public:
    using PlanNodePtr = std::shared_ptr<PlanNode>;
public:

    PlanNode(Settings &  settings_ , Context * context_  ):settings(settings_),context(context_){

    };
    PlanNode(Settings &  settings_  ):settings(settings_){

    };
    PlanNode();
    virtual ~PlanNode();

    //virtual void serialize(WriteBuffer & ostr) ;
    //virtual void deserialze(ReadBuffer & istr) ;

    void addChild(PlanNodePtr child){
        childs.push_back(child);
      //  child->setFather(this);
    }
    //void setFather(PlanNode *father_){ father  = father_;}
    void cleanChild();

    void setUnaryChild();
    void setLeftChild();
    void setRightChild();

    virtual  std::string  type();
    //virtual  Block getHeader();
    virtual std::shared_ptr<ExecNode>  createExecNode() ;
    std::string getName() ;

    Block getHeader() {
        auto execNode =  createExecNode();
        execNode->readPrefix();
        return  execNode->getHeader();
    }



    PlanNodePtr getUnaryChild() { return  childs[0]; }

    std::vector<PlanNodePtr> getChilds () { return childs ;}

    void setChild(PlanNodePtr child ,int index) { childs[index] = child;}
    PlanNodePtr getChild(int index) { return childs[index];}
    virtual int  exechangeCost() { return  0 ;}
    //virtual void initDistribution();
    //virtual void initDistribution(std::shared_ptr<Distribution>  distribution);
    std::shared_ptr<Distribution> getDistribution()  { return distribution;} // data flow  after the planNode ,what data's distribution

    void setDistribution(std::shared_ptr<Distribution>  distribution_) { distribution = distribution_ ;}
protected:
    Settings settings  ;
    Context * context ;
    std::shared_ptr<Distribution> distribution;
private:

    std::vector<PlanNodePtr> childs;
    Block  header;
    //PlanNode *   father ;



};


}
