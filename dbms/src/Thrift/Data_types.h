/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef Data_TYPES_H
#define Data_TYPES_H

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <thrift/cxxfunctional.h>
#include "Types_types.h"


namespace palo {

typedef struct _TRowBatch__isset {
  _TRowBatch__isset() : tuple_offsets(false), tuple_data(false), is_compressed(false), be_number(false), packet_seq(false) {}
  bool tuple_offsets;
  bool tuple_data;
  bool is_compressed;
  bool be_number;
  bool packet_seq;
} _TRowBatch__isset;

class TRowBatch {
 public:

  static const char* ascii_fingerprint; // = "B70BEE93903C8C38327CD1A167402ADC";
  static const uint8_t binary_fingerprint[16]; // = {0xB7,0x0B,0xEE,0x93,0x90,0x3C,0x8C,0x38,0x32,0x7C,0xD1,0xA1,0x67,0x40,0x2A,0xDC};

  TRowBatch() : num_rows(0), tuple_data(), is_compressed(0), be_number(0), packet_seq(0) {
  }

  virtual ~TRowBatch() throw() {}

  int32_t num_rows;
  std::vector< ::palo::TTupleId>  row_tuples;
  std::vector<int32_t>  tuple_offsets;
  std::string tuple_data;
  bool is_compressed;
  int32_t be_number;
  int64_t packet_seq;

  _TRowBatch__isset __isset;

  void __set_num_rows(const int32_t val) {
    num_rows = val;
  }

  void __set_row_tuples(const std::vector< ::palo::TTupleId> & val) {
    row_tuples = val;
  }

  void __set_tuple_offsets(const std::vector<int32_t> & val) {
    tuple_offsets = val;
  }

  void __set_tuple_data(const std::string& val) {
    tuple_data = val;
  }

  void __set_is_compressed(const bool val) {
    is_compressed = val;
  }

  void __set_be_number(const int32_t val) {
    be_number = val;
  }

  void __set_packet_seq(const int64_t val) {
    packet_seq = val;
  }

  bool operator == (const TRowBatch & rhs) const
  {
    if (!(num_rows == rhs.num_rows))
      return false;
    if (!(row_tuples == rhs.row_tuples))
      return false;
    if (!(tuple_offsets == rhs.tuple_offsets))
      return false;
    if (!(tuple_data == rhs.tuple_data))
      return false;
    if (!(is_compressed == rhs.is_compressed))
      return false;
    if (!(be_number == rhs.be_number))
      return false;
    if (!(packet_seq == rhs.packet_seq))
      return false;
    return true;
  }
  bool operator != (const TRowBatch &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TRowBatch & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TRowBatch &a, TRowBatch &b);

typedef struct _TColumnValue__isset {
  _TColumnValue__isset() : boolVal(false), intVal(false), longVal(false), doubleVal(false), stringVal(false) {}
  bool boolVal;
  bool intVal;
  bool longVal;
  bool doubleVal;
  bool stringVal;
} _TColumnValue__isset;

class TColumnValue {
 public:

  static const char* ascii_fingerprint; // = "4B58FCC987D8961AD547D15D96292286";
  static const uint8_t binary_fingerprint[16]; // = {0x4B,0x58,0xFC,0xC9,0x87,0xD8,0x96,0x1A,0xD5,0x47,0xD1,0x5D,0x96,0x29,0x22,0x86};

  TColumnValue() : boolVal(0), intVal(0), longVal(0), doubleVal(0), stringVal() {
  }

  virtual ~TColumnValue() throw() {}

  bool boolVal;
  int32_t intVal;
  int64_t longVal;
  double doubleVal;
  std::string stringVal;

  _TColumnValue__isset __isset;

  void __set_boolVal(const bool val) {
    boolVal = val;
    __isset.boolVal = true;
  }

  void __set_intVal(const int32_t val) {
    intVal = val;
    __isset.intVal = true;
  }

  void __set_longVal(const int64_t val) {
    longVal = val;
    __isset.longVal = true;
  }

  void __set_doubleVal(const double val) {
    doubleVal = val;
    __isset.doubleVal = true;
  }

  void __set_stringVal(const std::string& val) {
    stringVal = val;
    __isset.stringVal = true;
  }

  bool operator == (const TColumnValue & rhs) const
  {
    if (__isset.boolVal != rhs.__isset.boolVal)
      return false;
    else if (__isset.boolVal && !(boolVal == rhs.boolVal))
      return false;
    if (__isset.intVal != rhs.__isset.intVal)
      return false;
    else if (__isset.intVal && !(intVal == rhs.intVal))
      return false;
    if (__isset.longVal != rhs.__isset.longVal)
      return false;
    else if (__isset.longVal && !(longVal == rhs.longVal))
      return false;
    if (__isset.doubleVal != rhs.__isset.doubleVal)
      return false;
    else if (__isset.doubleVal && !(doubleVal == rhs.doubleVal))
      return false;
    if (__isset.stringVal != rhs.__isset.stringVal)
      return false;
    else if (__isset.stringVal && !(stringVal == rhs.stringVal))
      return false;
    return true;
  }
  bool operator != (const TColumnValue &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TColumnValue & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TColumnValue &a, TColumnValue &b);

typedef struct _TResultRow__isset {
  _TResultRow__isset() : colVals(false) {}
  bool colVals;
} _TResultRow__isset;

class TResultRow {
 public:

  static const char* ascii_fingerprint; // = "0D54948F5664A75A62C0B09D25FD9C46";
  static const uint8_t binary_fingerprint[16]; // = {0x0D,0x54,0x94,0x8F,0x56,0x64,0xA7,0x5A,0x62,0xC0,0xB0,0x9D,0x25,0xFD,0x9C,0x46};

  TResultRow() {
  }

  virtual ~TResultRow() throw() {}

  std::vector<TColumnValue>  colVals;

  _TResultRow__isset __isset;

  void __set_colVals(const std::vector<TColumnValue> & val) {
    colVals = val;
  }

  bool operator == (const TResultRow & rhs) const
  {
    if (!(colVals == rhs.colVals))
      return false;
    return true;
  }
  bool operator != (const TResultRow &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TResultRow & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TResultRow &a, TResultRow &b);


class TResultBatch {
 public:

  static const char* ascii_fingerprint; // = "89DF93A691A355C4E23AED43981EDBF5";
  static const uint8_t binary_fingerprint[16]; // = {0x89,0xDF,0x93,0xA6,0x91,0xA3,0x55,0xC4,0xE2,0x3A,0xED,0x43,0x98,0x1E,0xDB,0xF5};

  TResultBatch() : is_compressed(0), packet_seq(0) {
  }

  virtual ~TResultBatch() throw() {}

  std::vector<std::string>  rows;
  bool is_compressed;
  int64_t packet_seq;

  void __set_rows(const std::vector<std::string> & val) {
    rows = val;
  }

  void __set_is_compressed(const bool val) {
    is_compressed = val;
  }

  void __set_packet_seq(const int64_t val) {
    packet_seq = val;
  }

  bool operator == (const TResultBatch & rhs) const
  {
    if (!(rows == rhs.rows))
      return false;
    if (!(is_compressed == rhs.is_compressed))
      return false;
    if (!(packet_seq == rhs.packet_seq))
      return false;
    return true;
  }
  bool operator != (const TResultBatch &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TResultBatch & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TResultBatch &a, TResultBatch &b);

} // namespace

#endif
