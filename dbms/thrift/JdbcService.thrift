
namespace cpp palo
namespace java com.baidu.palo.thrift
include "PaloInternalService.thrift"
enum TJdbcServerCode {
  OK,
  ERROR
 }



service JdbcService{

    list<string>  submitQuery (1:string query) ; //by spark  return  sql be address hosts

    //PaloInternalService.TExecPlanFragmentParams  getExecPara();
}
