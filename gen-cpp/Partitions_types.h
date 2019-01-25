/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#ifndef Partitions_TYPES_H
#define Partitions_TYPES_H

#include <thrift/Thrift.h>
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/transport/TTransport.h>

#include <thrift/cxxfunctional.h>
#include "Exprs_types.h"
#include "Types_types.h"


namespace palo {

struct TPartitionType {
  enum type {
    UNPARTITIONED = 0,
    RANDOM = 1,
    HASH_PARTITIONED = 2,
    RANGE_PARTITIONED = 3
  };
};

extern const std::map<int, const char*> _TPartitionType_VALUES_TO_NAMES;

struct TDistributionType {
  enum type {
    UNPARTITIONED = 0,
    RANDOM = 1,
    HASH_PARTITIONED = 2
  };
};

extern const std::map<int, const char*> _TDistributionType_VALUES_TO_NAMES;

typedef struct _TPartitionKey__isset {
  _TPartitionKey__isset() : type(false), key(false) {}
  bool type;
  bool key;
} _TPartitionKey__isset;

class TPartitionKey {
 public:

  static const char* ascii_fingerprint; // = "A998E9FEC66BC6A846AD90EFAE4F1521";
  static const uint8_t binary_fingerprint[16]; // = {0xA9,0x98,0xE9,0xFE,0xC6,0x6B,0xC6,0xA8,0x46,0xAD,0x90,0xEF,0xAE,0x4F,0x15,0x21};

  TPartitionKey() : sign(0), type(( ::palo::TPrimitiveType::type)0), key() {
  }

  virtual ~TPartitionKey() throw() {}

  int16_t sign;
   ::palo::TPrimitiveType::type type;
  std::string key;

  _TPartitionKey__isset __isset;

  void __set_sign(const int16_t val) {
    sign = val;
  }

  void __set_type(const  ::palo::TPrimitiveType::type val) {
    type = val;
    __isset.type = true;
  }

  void __set_key(const std::string& val) {
    key = val;
    __isset.key = true;
  }

  bool operator == (const TPartitionKey & rhs) const
  {
    if (!(sign == rhs.sign))
      return false;
    if (__isset.type != rhs.__isset.type)
      return false;
    else if (__isset.type && !(type == rhs.type))
      return false;
    if (__isset.key != rhs.__isset.key)
      return false;
    else if (__isset.key && !(key == rhs.key))
      return false;
    return true;
  }
  bool operator != (const TPartitionKey &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TPartitionKey & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TPartitionKey &a, TPartitionKey &b);


class TPartitionRange {
 public:

  static const char* ascii_fingerprint; // = "700246BAC5AD111808294FD9C6A0ECE3";
  static const uint8_t binary_fingerprint[16]; // = {0x70,0x02,0x46,0xBA,0xC5,0xAD,0x11,0x18,0x08,0x29,0x4F,0xD9,0xC6,0xA0,0xEC,0xE3};

  TPartitionRange() : include_start_key(0), include_end_key(0) {
  }

  virtual ~TPartitionRange() throw() {}

  TPartitionKey start_key;
  TPartitionKey end_key;
  bool include_start_key;
  bool include_end_key;

  void __set_start_key(const TPartitionKey& val) {
    start_key = val;
  }

  void __set_end_key(const TPartitionKey& val) {
    end_key = val;
  }

  void __set_include_start_key(const bool val) {
    include_start_key = val;
  }

  void __set_include_end_key(const bool val) {
    include_end_key = val;
  }

  bool operator == (const TPartitionRange & rhs) const
  {
    if (!(start_key == rhs.start_key))
      return false;
    if (!(end_key == rhs.end_key))
      return false;
    if (!(include_start_key == rhs.include_start_key))
      return false;
    if (!(include_end_key == rhs.include_end_key))
      return false;
    return true;
  }
  bool operator != (const TPartitionRange &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TPartitionRange & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TPartitionRange &a, TPartitionRange &b);

typedef struct _TRangePartition__isset {
  _TRangePartition__isset() : distributed_exprs(false), distribute_bucket(false) {}
  bool distributed_exprs;
  bool distribute_bucket;
} _TRangePartition__isset;

class TRangePartition {
 public:

  static const char* ascii_fingerprint; // = "C58DB820E64943D3E08F8F8B6BE566AB";
  static const uint8_t binary_fingerprint[16]; // = {0xC5,0x8D,0xB8,0x20,0xE6,0x49,0x43,0xD3,0xE0,0x8F,0x8F,0x8B,0x6B,0xE5,0x66,0xAB};

  TRangePartition() : partition_id(0), distribute_bucket(0) {
  }

  virtual ~TRangePartition() throw() {}

  int64_t partition_id;
  TPartitionRange range;
  std::vector< ::palo::TExpr>  distributed_exprs;
  int32_t distribute_bucket;

  _TRangePartition__isset __isset;

  void __set_partition_id(const int64_t val) {
    partition_id = val;
  }

  void __set_range(const TPartitionRange& val) {
    range = val;
  }

  void __set_distributed_exprs(const std::vector< ::palo::TExpr> & val) {
    distributed_exprs = val;
    __isset.distributed_exprs = true;
  }

  void __set_distribute_bucket(const int32_t val) {
    distribute_bucket = val;
    __isset.distribute_bucket = true;
  }

  bool operator == (const TRangePartition & rhs) const
  {
    if (!(partition_id == rhs.partition_id))
      return false;
    if (!(range == rhs.range))
      return false;
    if (__isset.distributed_exprs != rhs.__isset.distributed_exprs)
      return false;
    else if (__isset.distributed_exprs && !(distributed_exprs == rhs.distributed_exprs))
      return false;
    if (__isset.distribute_bucket != rhs.__isset.distribute_bucket)
      return false;
    else if (__isset.distribute_bucket && !(distribute_bucket == rhs.distribute_bucket))
      return false;
    return true;
  }
  bool operator != (const TRangePartition &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TRangePartition & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TRangePartition &a, TRangePartition &b);

typedef struct _TDataPartition__isset {
  _TDataPartition__isset() : partition_exprs(false), partition_infos(false) {}
  bool partition_exprs;
  bool partition_infos;
} _TDataPartition__isset;

class TDataPartition {
 public:

  static const char* ascii_fingerprint; // = "5E1D1692EF71F1CA608FB155498ABF8A";
  static const uint8_t binary_fingerprint[16]; // = {0x5E,0x1D,0x16,0x92,0xEF,0x71,0xF1,0xCA,0x60,0x8F,0xB1,0x55,0x49,0x8A,0xBF,0x8A};

  TDataPartition() : type((TPartitionType::type)0) {
  }

  virtual ~TDataPartition() throw() {}

  TPartitionType::type type;
  std::vector< ::palo::TExpr>  partition_exprs;
  std::vector<TRangePartition>  partition_infos;

  _TDataPartition__isset __isset;

  void __set_type(const TPartitionType::type val) {
    type = val;
  }

  void __set_partition_exprs(const std::vector< ::palo::TExpr> & val) {
    partition_exprs = val;
    __isset.partition_exprs = true;
  }

  void __set_partition_infos(const std::vector<TRangePartition> & val) {
    partition_infos = val;
    __isset.partition_infos = true;
  }

  bool operator == (const TDataPartition & rhs) const
  {
    if (!(type == rhs.type))
      return false;
    if (__isset.partition_exprs != rhs.__isset.partition_exprs)
      return false;
    else if (__isset.partition_exprs && !(partition_exprs == rhs.partition_exprs))
      return false;
    if (__isset.partition_infos != rhs.__isset.partition_infos)
      return false;
    else if (__isset.partition_infos && !(partition_infos == rhs.partition_infos))
      return false;
    return true;
  }
  bool operator != (const TDataPartition &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const TDataPartition & ) const;

  uint32_t read(::apache::thrift::protocol::TProtocol* iprot);
  uint32_t write(::apache::thrift::protocol::TProtocol* oprot) const;

};

void swap(TDataPartition &a, TDataPartition &b);

} // namespace

#endif
