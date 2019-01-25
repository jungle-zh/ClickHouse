
namespace cpp palo
namespace java com.baidu.palo.thrift
include "PaloInternalService.thrift"
include "Types.thrift"
enum TShardServerCode {
  OK,
  ERROR
 }
 struct TJdbcShardParam {

  1: required  PaloInternalService.TExecPlanFragmentParams planFragmentParam;

  2: required  string querySql;

  3: required  i64 taken;

  4: required   list<string> columnName;

  5: required   list<Types.TPrimitiveType>  columnTypes;

 }



service ShardService{

    //list<string>  submitQuery (1:string query) ; //by spark  return  sql be address hosts

    //PaloInternalService.TExecPlanFragmentParams  getExecPara();

    TShardServerCode sendPlanFragmentParam(1:TJdbcShardParam param);
}
