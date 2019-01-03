#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Common/ConcurrentBoundedQueue.h>


namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemoryFifo : public ext::shared_ptr_helper<StorageMemoryFifo>, public IStorage
{
friend class MemoryBlockInputFifoStream;
friend class MemoryBlockOutputFifoStream;

public:
    std::string getName() const override { return "MemoryFifo"; }
    std::string getTableName() const override { return table_name; }

    size_t getSize()  { return data.size(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const Settings & settings) override;

    void drop() override;
    void rename(const String & /*new_path_to_db*/, const String & /*new_database_name*/, const String & new_table_name) override { table_name = new_table_name; }


    bool writeDone()  {
        return  data.getPushDone();
    }

    bool readDone() {
        return  data.getPushDone() && data.curPushCnt() == data.curPopCnt() && data.size() == 0;
    }



private:
    String table_name;
    ConcurrentBoundedQueue<Block> data ;

    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
   // BlocksList data;


     //std::mutex mutex;

protected:
    StorageMemoryFifo(String table_name_, ColumnsDescription columns_description_ , size_t  max_block_size);
};

}
