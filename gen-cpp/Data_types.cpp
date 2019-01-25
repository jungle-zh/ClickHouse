/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
#include "Data_types.h"

#include <algorithm>

namespace palo {

const char* TRowBatch::ascii_fingerprint = "B70BEE93903C8C38327CD1A167402ADC";
const uint8_t TRowBatch::binary_fingerprint[16] = {0xB7,0x0B,0xEE,0x93,0x90,0x3C,0x8C,0x38,0x32,0x7C,0xD1,0xA1,0x67,0x40,0x2A,0xDC};

uint32_t TRowBatch::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  bool isset_num_rows = false;
  bool isset_row_tuples = false;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->num_rows);
          isset_num_rows = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->row_tuples.clear();
            uint32_t _size0;
            ::apache::thrift::protocol::TType _etype3;
            xfer += iprot->readListBegin(_etype3, _size0);
            this->row_tuples.resize(_size0);
            uint32_t _i4;
            for (_i4 = 0; _i4 < _size0; ++_i4)
            {
              xfer += iprot->readI32(this->row_tuples[_i4]);
            }
            xfer += iprot->readListEnd();
          }
          isset_row_tuples = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->tuple_offsets.clear();
            uint32_t _size5;
            ::apache::thrift::protocol::TType _etype8;
            xfer += iprot->readListBegin(_etype8, _size5);
            this->tuple_offsets.resize(_size5);
            uint32_t _i9;
            for (_i9 = 0; _i9 < _size5; ++_i9)
            {
              xfer += iprot->readI32(this->tuple_offsets[_i9]);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.tuple_offsets = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->tuple_data);
          this->__isset.tuple_data = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->is_compressed);
          this->__isset.is_compressed = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 6:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->be_number);
          this->__isset.be_number = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 7:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->packet_seq);
          this->__isset.packet_seq = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  if (!isset_num_rows)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  if (!isset_row_tuples)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  return xfer;
}

uint32_t TRowBatch::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TRowBatch");

  xfer += oprot->writeFieldBegin("num_rows", ::apache::thrift::protocol::T_I32, 1);
  xfer += oprot->writeI32(this->num_rows);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("row_tuples", ::apache::thrift::protocol::T_LIST, 2);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(this->row_tuples.size()));
    std::vector< ::palo::TTupleId> ::const_iterator _iter10;
    for (_iter10 = this->row_tuples.begin(); _iter10 != this->row_tuples.end(); ++_iter10)
    {
      xfer += oprot->writeI32((*_iter10));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("tuple_offsets", ::apache::thrift::protocol::T_LIST, 3);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(this->tuple_offsets.size()));
    std::vector<int32_t> ::const_iterator _iter11;
    for (_iter11 = this->tuple_offsets.begin(); _iter11 != this->tuple_offsets.end(); ++_iter11)
    {
      xfer += oprot->writeI32((*_iter11));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("tuple_data", ::apache::thrift::protocol::T_STRING, 4);
  xfer += oprot->writeString(this->tuple_data);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("is_compressed", ::apache::thrift::protocol::T_BOOL, 5);
  xfer += oprot->writeBool(this->is_compressed);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("be_number", ::apache::thrift::protocol::T_I32, 6);
  xfer += oprot->writeI32(this->be_number);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("packet_seq", ::apache::thrift::protocol::T_I64, 7);
  xfer += oprot->writeI64(this->packet_seq);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(TRowBatch &a, TRowBatch &b) {
  using ::std::swap;
  swap(a.num_rows, b.num_rows);
  swap(a.row_tuples, b.row_tuples);
  swap(a.tuple_offsets, b.tuple_offsets);
  swap(a.tuple_data, b.tuple_data);
  swap(a.is_compressed, b.is_compressed);
  swap(a.be_number, b.be_number);
  swap(a.packet_seq, b.packet_seq);
  swap(a.__isset, b.__isset);
}

const char* TColumnValue::ascii_fingerprint = "4B58FCC987D8961AD547D15D96292286";
const uint8_t TColumnValue::binary_fingerprint[16] = {0x4B,0x58,0xFC,0xC9,0x87,0xD8,0x96,0x1A,0xD5,0x47,0xD1,0x5D,0x96,0x29,0x22,0x86};

uint32_t TColumnValue::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->boolVal);
          this->__isset.boolVal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_I32) {
          xfer += iprot->readI32(this->intVal);
          this->__isset.intVal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->longVal);
          this->__isset.longVal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 4:
        if (ftype == ::apache::thrift::protocol::T_DOUBLE) {
          xfer += iprot->readDouble(this->doubleVal);
          this->__isset.doubleVal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 5:
        if (ftype == ::apache::thrift::protocol::T_STRING) {
          xfer += iprot->readString(this->stringVal);
          this->__isset.stringVal = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t TColumnValue::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TColumnValue");

  if (this->__isset.boolVal) {
    xfer += oprot->writeFieldBegin("boolVal", ::apache::thrift::protocol::T_BOOL, 1);
    xfer += oprot->writeBool(this->boolVal);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.intVal) {
    xfer += oprot->writeFieldBegin("intVal", ::apache::thrift::protocol::T_I32, 2);
    xfer += oprot->writeI32(this->intVal);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.longVal) {
    xfer += oprot->writeFieldBegin("longVal", ::apache::thrift::protocol::T_I64, 3);
    xfer += oprot->writeI64(this->longVal);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.doubleVal) {
    xfer += oprot->writeFieldBegin("doubleVal", ::apache::thrift::protocol::T_DOUBLE, 4);
    xfer += oprot->writeDouble(this->doubleVal);
    xfer += oprot->writeFieldEnd();
  }
  if (this->__isset.stringVal) {
    xfer += oprot->writeFieldBegin("stringVal", ::apache::thrift::protocol::T_STRING, 5);
    xfer += oprot->writeString(this->stringVal);
    xfer += oprot->writeFieldEnd();
  }
  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(TColumnValue &a, TColumnValue &b) {
  using ::std::swap;
  swap(a.boolVal, b.boolVal);
  swap(a.intVal, b.intVal);
  swap(a.longVal, b.longVal);
  swap(a.doubleVal, b.doubleVal);
  swap(a.stringVal, b.stringVal);
  swap(a.__isset, b.__isset);
}

const char* TResultRow::ascii_fingerprint = "0D54948F5664A75A62C0B09D25FD9C46";
const uint8_t TResultRow::binary_fingerprint[16] = {0x0D,0x54,0x94,0x8F,0x56,0x64,0xA7,0x5A,0x62,0xC0,0xB0,0x9D,0x25,0xFD,0x9C,0x46};

uint32_t TResultRow::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;


  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->colVals.clear();
            uint32_t _size12;
            ::apache::thrift::protocol::TType _etype15;
            xfer += iprot->readListBegin(_etype15, _size12);
            this->colVals.resize(_size12);
            uint32_t _i16;
            for (_i16 = 0; _i16 < _size12; ++_i16)
            {
              xfer += this->colVals[_i16].read(iprot);
            }
            xfer += iprot->readListEnd();
          }
          this->__isset.colVals = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  return xfer;
}

uint32_t TResultRow::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TResultRow");

  xfer += oprot->writeFieldBegin("colVals", ::apache::thrift::protocol::T_LIST, 1);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(this->colVals.size()));
    std::vector<TColumnValue> ::const_iterator _iter17;
    for (_iter17 = this->colVals.begin(); _iter17 != this->colVals.end(); ++_iter17)
    {
      xfer += (*_iter17).write(oprot);
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(TResultRow &a, TResultRow &b) {
  using ::std::swap;
  swap(a.colVals, b.colVals);
  swap(a.__isset, b.__isset);
}

const char* TResultBatch::ascii_fingerprint = "89DF93A691A355C4E23AED43981EDBF5";
const uint8_t TResultBatch::binary_fingerprint[16] = {0x89,0xDF,0x93,0xA6,0x91,0xA3,0x55,0xC4,0xE2,0x3A,0xED,0x43,0x98,0x1E,0xDB,0xF5};

uint32_t TResultBatch::read(::apache::thrift::protocol::TProtocol* iprot) {

  uint32_t xfer = 0;
  std::string fname;
  ::apache::thrift::protocol::TType ftype;
  int16_t fid;

  xfer += iprot->readStructBegin(fname);

  using ::apache::thrift::protocol::TProtocolException;

  bool isset_rows = false;
  bool isset_is_compressed = false;
  bool isset_packet_seq = false;

  while (true)
  {
    xfer += iprot->readFieldBegin(fname, ftype, fid);
    if (ftype == ::apache::thrift::protocol::T_STOP) {
      break;
    }
    switch (fid)
    {
      case 1:
        if (ftype == ::apache::thrift::protocol::T_LIST) {
          {
            this->rows.clear();
            uint32_t _size18;
            ::apache::thrift::protocol::TType _etype21;
            xfer += iprot->readListBegin(_etype21, _size18);
            this->rows.resize(_size18);
            uint32_t _i22;
            for (_i22 = 0; _i22 < _size18; ++_i22)
            {
              xfer += iprot->readBinary(this->rows[_i22]);
            }
            xfer += iprot->readListEnd();
          }
          isset_rows = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 2:
        if (ftype == ::apache::thrift::protocol::T_BOOL) {
          xfer += iprot->readBool(this->is_compressed);
          isset_is_compressed = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      case 3:
        if (ftype == ::apache::thrift::protocol::T_I64) {
          xfer += iprot->readI64(this->packet_seq);
          isset_packet_seq = true;
        } else {
          xfer += iprot->skip(ftype);
        }
        break;
      default:
        xfer += iprot->skip(ftype);
        break;
    }
    xfer += iprot->readFieldEnd();
  }

  xfer += iprot->readStructEnd();

  if (!isset_rows)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  if (!isset_is_compressed)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  if (!isset_packet_seq)
    throw TProtocolException(TProtocolException::INVALID_DATA);
  return xfer;
}

uint32_t TResultBatch::write(::apache::thrift::protocol::TProtocol* oprot) const {
  uint32_t xfer = 0;
  xfer += oprot->writeStructBegin("TResultBatch");

  xfer += oprot->writeFieldBegin("rows", ::apache::thrift::protocol::T_LIST, 1);
  {
    xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(this->rows.size()));
    std::vector<std::string> ::const_iterator _iter23;
    for (_iter23 = this->rows.begin(); _iter23 != this->rows.end(); ++_iter23)
    {
      xfer += oprot->writeBinary((*_iter23));
    }
    xfer += oprot->writeListEnd();
  }
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("is_compressed", ::apache::thrift::protocol::T_BOOL, 2);
  xfer += oprot->writeBool(this->is_compressed);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldBegin("packet_seq", ::apache::thrift::protocol::T_I64, 3);
  xfer += oprot->writeI64(this->packet_seq);
  xfer += oprot->writeFieldEnd();

  xfer += oprot->writeFieldStop();
  xfer += oprot->writeStructEnd();
  return xfer;
}

void swap(TResultBatch &a, TResultBatch &b) {
  using ::std::swap;
  swap(a.rows, b.rows);
  swap(a.is_compressed, b.is_compressed);
  swap(a.packet_seq, b.packet_seq);
}

} // namespace
