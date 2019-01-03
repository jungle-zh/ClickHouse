#include <Core/Types.h>
#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/VarInt.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/BlockInfo.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_BLOCK_INFO_FIELD;
}


/// Write values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
void BlockInfo::write(WriteBuffer & out) const
{


    LOG_DEBUG(&Logger::get("BlockInfo"),"write");

 /// Set of pairs `FIELD_NUM`, value in binary form. Then 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    LOG_DEBUG(&Logger::get("BlockInfo"),"write field_num is " + std::to_string(FIELD_NUM)); \
    writeVarUInt(FIELD_NUM, out); \
    writeBinary(NAME, out);

    APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD);

#undef WRITE_FIELD
    writeVarUInt(0, out);
}

/// Read values in binary form.
void BlockInfo::read(ReadBuffer & in)
{
    UInt64 field_num = 0;

    LOG_DEBUG(&Logger::get("BlockInfo"),"read");
    while (true)
    {
        readVarUInt(field_num, in);
        LOG_DEBUG(&Logger::get("BlockInfo"),"read field_num is " + std::to_string(field_num));
        if (field_num == 0)
            break;

        switch (field_num)
        {
        #define READ_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
            case FIELD_NUM: \
                readBinary(NAME, in); \
                break;

            APPLY_FOR_BLOCK_INFO_FIELDS(READ_FIELD);

        #undef READ_FIELD
            default:
                throw Exception("Unknown BlockInfo field number: " + toString(field_num), ErrorCodes::UNKNOWN_BLOCK_INFO_FIELD);
        }
    }
}

}
