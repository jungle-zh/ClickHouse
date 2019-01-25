
namespace cpp palo
namespace java com.baidu.palo.thrift

include  "Descriptors.thrift"
include  "Types.thrift"
include  "DataSinks.thrift"



struct TExecStreaming {

  1: required Descriptors.TDescriptorTable desc_tbl

  2: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  3: required list<bool> nullable_tuples

  4: required DataSinks.TDataSplitSink split_sink


}