//
// Created by Administrator on 2019/5/4.
//

#include <Interpreters/PlanNode/ScanPlanNode.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/ExecNode/ScanExecNode.h>
#include <Storages/IStorage.h>

namespace DB {


    std::shared_ptr<ExecNode> ScanPlanNode::createExecNode() {

        Context * context = NULL;
        std::vector<std::string> required_column_;
        for(auto col : required_column){
            required_column_.push_back(col);
        }
        return   std::make_shared<ScanExecNode>(dbName,tableName,required_column_,query,context);

    }
     Block ScanPlanNode::getHeader() {
     if(tableName == "stu"){


         const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
         std::string name_type  = "String";
         std::string age_type = "UInt32";

         Block block;
         ColumnWithTypeAndName stuName;
         stuName.type = data_type_factory.get(name_type);
         stuName.name = "name";

         ColumnWithTypeAndName stuAge;
         stuAge.type = data_type_factory.get(age_type);
         stuAge.name = "age";

         block.insert(stuName);
         block.insert(stuAge);
         return  block;
     } else if(tableName == "score"){

         const DataTypeFactory & data_type_factory = DataTypeFactory::instance();
         std::string name_type  = "String";
         std::string age_type = "UInt32";

         Block block;
         ColumnWithTypeAndName stuName;
         stuName.type = data_type_factory.get(name_type);
         stuName.name = "name";

         ColumnWithTypeAndName stuScore;
         stuScore.type = data_type_factory.get(age_type);
         stuScore.name = "score";

         block.insert(stuName);
         block.insert(stuScore);

         return block;

     } else{
         auto storage = context->getTable(dbName, tableName);
         Block all =  storage->getSampleBlock();
         ColumnsWithTypeAndName cols;
         for(auto col : required_column){
             ColumnWithTypeAndName c = all.getByName(col);
             cols.push_back(c);
         }

         Block block(cols);
         return  block;
     }
 }

}
