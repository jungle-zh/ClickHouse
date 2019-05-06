//
// Created by Administrator on 2019/5/3.
//

#include <Interpreters/PlanNode/SortMergeNode.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
namespace DB {

    Block SortMergeNode::read() { //
        if (!impl)
        {
            while (Block block = getUnaryChild()->read())
            {
                /// If there were only const columns in sort description, then there is no need to sort.
                /// Return the blocks as is.
                if (description.empty())
                    return block;

                removeConstantsFromBlock(block);

                blocks.push_back(block);
                sum_bytes_in_blocks += block.bytes();

                /** If too many of them and if external sorting is enabled,
                  *  will merge blocks that we have in memory at this moment and write merged stream to temporary (compressed) file.
                  * NOTE. It's possible to check free space in filesystem.
                  */
                if (max_bytes_before_external_sort && sum_bytes_in_blocks > max_bytes_before_external_sort)
                {
                    temporary_files.emplace_back(new Poco::TemporaryFile(tmp_path));
                    const std::string & path = temporary_files.back()->path();
                    WriteBufferFromFile file_buf(path);
                    CompressedWriteBuffer compressed_buf(file_buf);
                    NativeBlockOutputStream block_out(compressed_buf, 0, header_without_constants);
                    MergeSortingBlocksBlockInputStream block_in(blocks, description, max_merged_block_size, limit);

                    LOG_INFO(log, "Sorting and writing part of data into temporary file " + path);
                    ProfileEvents::increment(ProfileEvents::ExternalSortWritePart);
                    copyData(block_in, block_out, &is_cancelled);    /// NOTE. Possibly limit disk usage.
                    LOG_INFO(log, "Done writing part of data into temporary file " + path);

                    blocks.clear();
                    sum_bytes_in_blocks = 0;
                }
            }

            if ((blocks.empty() && temporary_files.empty()) || isCancelledOrThrowIfKilled())
                return Block();

            if (temporary_files.empty())
            {
                impl = std::make_unique<MergeSortingBlocksBlockInputStream>(blocks, description, max_merged_block_size, limit);
            }
            else
            {
                /// If there was temporary files.
                ProfileEvents::increment(ProfileEvents::ExternalSortMerge);

                LOG_INFO(log, "There are " << temporary_files.size() << " temporary sorted parts to merge.");

                /// Create sorted streams to merge.
                for (const auto & file : temporary_files)
                {
                    temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path(), header_without_constants));
                    inputs_to_merge.emplace_back(temporary_inputs.back()->block_in);
                }

                /// Rest of blocks in memory.
                if (!blocks.empty())
                    inputs_to_merge.emplace_back(std::make_shared<MergeSortingBlocksBlockInputStream>(blocks, description, max_merged_block_size, limit));

                /// Will merge that sorted streams.
                impl = std::make_unique<MergingSortedBlockInputStream>(inputs_to_merge, description, max_merged_block_size, limit);
            }
        }

        Block res = impl->read();
        if (res)
            enrichBlockWithConstants(res, header);
        return res;
    }
}
