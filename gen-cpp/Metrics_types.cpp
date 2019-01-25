/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "Metrics_types.h"

#include <algorithm>

namespace palo {

int _kTUnitValues[] = {
  TUnit::UNIT,
  TUnit::UNIT_PER_SECOND,
  TUnit::CPU_TICKS,
  TUnit::BYTES,
  TUnit::BYTES_PER_SECOND,
  TUnit::TIME_NS,
  TUnit::DOUBLE_VALUE,
  TUnit::NONE,
  TUnit::TIME_MS,
  TUnit::TIME_S
};
const char* _kTUnitNames[] = {
  "UNIT",
  "UNIT_PER_SECOND",
  "CPU_TICKS",
  "BYTES",
  "BYTES_PER_SECOND",
  "TIME_NS",
  "DOUBLE_VALUE",
  "NONE",
  "TIME_MS",
  "TIME_S"
};
const std::map<int, const char*> _TUnit_VALUES_TO_NAMES(::apache::thrift::TEnumIterator(10, _kTUnitValues, _kTUnitNames), ::apache::thrift::TEnumIterator(-1, NULL, NULL));

int _kTMetricKindValues[] = {
  TMetricKind::GAUGE,
  TMetricKind::COUNTER,
  TMetricKind::PROPERTY,
  TMetricKind::STATS,
  TMetricKind::SET,
  TMetricKind::HISTOGRAM
};
const char* _kTMetricKindNames[] = {
  "GAUGE",
  "COUNTER",
  "PROPERTY",
  "STATS",
  "SET",
  "HISTOGRAM"
};
const std::map<int, const char*> _TMetricKind_VALUES_TO_NAMES(::apache::thrift::TEnumIterator(6, _kTMetricKindValues, _kTMetricKindNames), ::apache::thrift::TEnumIterator(-1, NULL, NULL));

} // namespace
