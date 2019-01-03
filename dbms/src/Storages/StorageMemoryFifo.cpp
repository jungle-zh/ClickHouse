#include <map>

#include <Common/Exception.h>

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/StorageMemoryFifo.h>
#include <Storages/StorageFactory.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class MemoryBlockInputFifoStream : public IProfilingBlockInputStream
{
public:
    MemoryBlockInputFifoStream(const Names & column_names_,   StorageMemoryFifo & storage_)
        : column_names(column_names_),  storage(storage_) {}

    String getName() const override { return "Memory"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        Block res;

        if(storage.readDone()){ //all finish

            //assert(storage.data.size() ==0 );
            LOG_DEBUG(&Logger::get("MemoryBlockInputFifoStream"),"storage fifo readDone");
            return  res;
        } else {
           while (!storage.data.tryPop(res,1000)){ //data not come yet

               LOG_WARNING(&Logger::get("StorageMemoryFifo"),"read storage fifo timeout");
           }
        }

        return  res;

    }
private:
    Names column_names;

    StorageMemoryFifo & storage;
};


class MemoryBlockOutputFifoStream : public IBlockOutputStream
{
public:
    explicit MemoryBlockOutputFifoStream(StorageMemoryFifo & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        LOG_DEBUG(&Logger::get("MemoryBlockOutputFifoStream") ,"write block ,name :" + block.dumpNames() + "  ,row size :" + std::to_string(block.rows()));

        if(!block){ // all write finish
            storage.data.setPushDone();
            LOG_DEBUG(&Logger::get("MemoryBlockOutputFifoStream"),"write storage fifo finish");
            return;
        }
        LOG_DEBUG(&Logger::get("MemoryBlockOutputFifoStream") ,"check 11");
        storage.check(block, true);
        //std::lock_guard<std::mutex> lock(storage.mutex);
        LOG_DEBUG(&Logger::get("MemoryBlockOutputFifoStream") ,"check 12");

        while (!storage.data.tryEmplace(1000,std::move(block))){
            LOG_WARNING(&Logger::get("MemoryBlockOutputFifoStream"),"write storage fifo timeout");
        }

        LOG_DEBUG(&Logger::get("MemoryBlockOutputFifoStream") ,"storage.data size :" + std::to_string(storage.data.size()));


    }
private:
    StorageMemoryFifo & storage;
};


StorageMemoryFifo::StorageMemoryFifo(String table_name_, ColumnsDescription columns_description_ , size_t max_block_size)
    : IStorage{std::move(columns_description_)}, table_name(std::move(table_name_)), data(max_block_size)
{
}


BlockInputStreams StorageMemoryFifo::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & processed_stage,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    std::string col_names_s  = "";
    for(auto & n : column_names){
        col_names_s += n;
        col_names_s += ",";
    }
    LOG_DEBUG(&Logger::get("StorageMemoryFifo"),"read column_names:" + col_names_s);
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;


    /**
    std::lock_guard<std::mutex> lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    BlockInputStreams res;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.push_back(std::make_shared<MemoryBlockInputFifoStream>(column_names, begin, end, *this));
    }

     **/
    BlockInputStreams res;
    for (size_t stream = 0; stream < num_streams; ++stream) {
        res.push_back(std::make_shared<MemoryBlockInputFifoStream>(column_names, *this));
    }



    return res;
}


BlockOutputStreamPtr StorageMemoryFifo::write(
    const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    LOG_DEBUG(&Logger::get("StorageMemoryFifo"),"write " );
    return std::make_shared<MemoryBlockOutputFifoStream>(*this);
}


void StorageMemoryFifo::drop()
{
    //std::lock_guard<std::mutex> lock(mutex);
    data.clear();
}


void registerStorageMemoryFifo(StorageFactory & factory)
{
    factory.registerStorage("MemoryFifo", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageMemoryFifo::create(args.table_name, args.columns,100000);
    });
}

}
