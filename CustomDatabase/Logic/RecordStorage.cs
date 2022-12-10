using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class RecordStorage : IRecordStorage
    {
        #region Variables
        private readonly IBlockStorage storage;

        private const int MaxRecordSize = 4194304; // 4MB
        private const int kNextBlockId = 0;
        private const int kRecordLength = 1;
        private const int kBlockContentLength = 2;
        private const int kPreviousBlockId = 3;
        private const int kIsDeleted = 4;
        #endregion Variables

        #region Constructor
        public RecordStorage (IBlockStorage storage)
        {
            if (storage == null)
            { throw new ArgumentNullException("storage"); }

            this.storage = storage;

            if (storage.BlockHeaderSize < 48)
            { throw new ArgumentException("Record storage needs at least 48 header bytes."); }
        }
        #endregion Constructor

        #region Methods (public)
        public virtual byte[] Find(uint recordId)
        {
            using (var block = storage.Find(recordId))
            {
                if (block == null)
                { return null; }

                // Ignore if block is deleted.
                if (1L == block.GetHeader(kIsDeleted))
                { return null; }

                // Ignore if block is a child.
                if (0L != block.GetHeader(kPreviousBlockId))
                { return null; }

                // Get total record size & allocate memory.
                long totalRecordSize = block.GetHeader(kRecordLength);

                if (totalRecordSize > MaxRecordSize)
                { throw new NotSupportedException("Unexpected record length: " + totalRecordSize); }

                byte[] data = new byte[totalRecordSize];
                int bytesRead = 0;

                // Filling the data block.
                IBlock currentBlock = block;

                while (true)
                {
                    uint nextBlockId;

                    using (currentBlock)
                    {
                        var thisBlockContentLength = currentBlock.GetHeader(kBlockContentLength);

                        if (thisBlockContentLength > storage.BlockContentSize)
                        { throw new Exception("Unexpected block content length: " + thisBlockContentLength); }

                        // Reading all content from current block
                        currentBlock.Read(destination: data, destinationOffset: bytesRead, sourceOffset: 0, count: (int)thisBlockContentLength);

                        bytesRead += (int)thisBlockContentLength;

                        nextBlockId = (uint)currentBlock.GetHeader(kNextBlockId);

                        if (nextBlockId == 0)
                        { return data; }
                    }

                    currentBlock = this.storage.Find(nextBlockId);

                    if (currentBlock == null)
                    { throw new Exception("Block not found by ID: " + nextBlockId); }
                }
            }
        }

        public virtual uint Create(Func<uint, byte[]> dataGenerator)
        {
            if (dataGenerator == null)
            { throw new ArgumentException(); }

            using (var firstBlock = AllocateBlock())
            {
                uint returnId = firstBlock.Id;

                byte[] data = dataGenerator(returnId);
                int dataWritten = 0;
                int dataToWrite = data.Length;

                firstBlock.SetHeader(kRecordLength, dataToWrite);

                if (dataToWrite == 0)
                { return returnId; }

                IBlock currentBlock = firstBlock;

                while (dataWritten < dataToWrite)
                {
                    IBlock nextBlock = null;

                    using (currentBlock)
                    {
                        int thisWrite = Math.Min(storage.BlockContentSize, dataToWrite - dataWritten);

                        currentBlock.Write(data, dataWritten, 0, thisWrite);
                        currentBlock.SetHeader(kBlockContentLength, (long)thisWrite);
                        dataWritten += thisWrite;

                        if (dataWritten < dataToWrite)
                        {
                            nextBlock = AllocateBlock();
                            bool isSuccess = false;

                            try
                            {
                                nextBlock.SetHeader(kPreviousBlockId, currentBlock.Id);
                                currentBlock.SetHeader(kNextBlockId, nextBlock.Id);
                                isSuccess = true;
                            }
                            finally
                            {
                                if (!isSuccess && nextBlock != null)
                                {
                                    nextBlock.Dispose();
                                    nextBlock = null;
                                }
                            }
                        }
                        else
                        { break; }
                    }

                    if (nextBlock != null)
                    { currentBlock = nextBlock; }
                }

                return returnId;
            }
        }

        public virtual uint Create(byte[] data)
        {
            if (data == null)
            { throw new ArgumentException(); }

            return Create(recordId => data);
        }

        public virtual uint Create()
        {
            using (var firstBlock = AllocateBlock())
            { return firstBlock.Id; }
        }

        public virtual void Delete(uint recordId)
        {
            using (var block = storage.Find(recordId))
            {
                IBlock currentBlock = block;

                while (true)
                {
                    IBlock nextBlock = null;

                    using (currentBlock)
                    {
                        MarkAsFree(currentBlock.Id);
                        currentBlock.SetHeader(kIsDeleted, 1L);

                        uint nextBlockId = (uint)currentBlock.GetHeader(kNextBlockId);

                        if (nextBlockId == 0)
                        { break; }
                        else
                        {
                            nextBlock = storage.Find(nextBlockId);

                            if (currentBlock == null)
                            { throw new Exception("Block not found by ID: " + nextBlockId); }
                        }
                    }

                    if (nextBlock != null)
                    { currentBlock = nextBlock; }
                }
            }
        }

        public virtual void Update(uint recordId, byte[] data)
        {
            int written = 0;
            int total = data.Length;
            var blocks = FindBlocks(recordId);
            int blocksUsed = 0;
            var previousBlock = (IBlock)null;

            try
            {
                while (written < total)
                {
                    int bytesToWrite = Math.Min(total - written, storage.BlockContentSize);
                    int blockIndex = (int)Math.Floor((double)written/(double)storage.BlockContentSize);

                    // If blockIndex exists within blocks, write into it
                    // If not, allocate new one for writing
                    var target = (IBlock)null;

                    if (blockIndex < blocks.Count())
                    { target = blocks[blockIndex]; }
                    else
                    {
                        target = AllocateBlock();

                        if (target == null)
                        { throw new Exception("Failed to allocate new block."); }

                        blocks.Add(target);
                    }

                    if (previousBlock != null)
                    {
                        previousBlock.SetHeader(kNextBlockId, target.Id);
                        target.SetHeader(kPreviousBlockId, previousBlock.Id);
                    }

                    target.Write(source: data, sourceOffset: written, destinationOffset: 0, count: bytesToWrite);
                    target.SetHeader(kBlockContentLength, bytesToWrite);
                    target.SetHeader(kNextBlockId, 0);

                    if (written == 0)
                    { target.SetHeader(kRecordLength, total); }

                    blocksUsed++;
                    written += bytesToWrite;
                    previousBlock = target;
                }

                // Remove any unused blocks
                if (blocksUsed < blocks.Count())
                {
                    for (int i = blocksUsed; i < blocks.Count(); i++)
                    { MarkAsFree(blocks[i].Id); }
                }
            }
            finally
            {
                foreach (var block in blocks)
                { block.Dispose(); }
            }
        }
        #endregion Methods (public)

        #region Methods (private)
        /// <summary>
        /// Find all blocks of a record and return in order
        /// </summary>
        /// <param name="recordId">Record identifier.</param>
        List<IBlock> FindBlocks(uint recordId)
        {
            var blocks = new List<IBlock>();
            bool isSuccess = false;

            try
            {
                uint currentBlockId = recordId;

                do
                {
                    var block = storage.Find(currentBlockId);

                    if (block == null)
                    {
                        // If block #0 was never created try doing it
                        if (currentBlockId == 0)
                        { block = storage.CreateNew(); }
                        else
                        { throw new Exception("Block not found by ID: " + currentBlockId); }
                    }

                    blocks.Add(block);

                    // Ignore the block if it's deleted
                    if (1L == block.GetHeader(kIsDeleted))
                    { throw new Exception("Block not found: " + currentBlockId); }

                    currentBlockId = (uint)block.GetHeader(kNextBlockId);
                } while (currentBlockId != 0);

                isSuccess = true;
                return blocks;
            }
            finally
            {
                if (!isSuccess)
                { 
                    // Emergency cleanup if something goes funky
                    foreach (var block in blocks)
                    { block.Dispose(); }
                }
            }
        }

        /// <summary>
        /// Allocate new block for use by reusing existing (non used) block or creating new one
        /// </summary>
        /// <returns>Newely allocated block.</returns>
        IBlock AllocateBlock()
        {
            uint reusableBlockId;
            IBlock newBlock;

            if (!TryFindFreeBlock(out reusableBlockId))
            {
                newBlock = storage.CreateNew();

                if (newBlock == null)
                { throw new Exception("Failed to create new block."); }
            }
            else
            {
                newBlock = storage.Find(reusableBlockId);

                if (newBlock == null)
                { throw new Exception("Block not found by ID: " + reusableBlockId); }

                newBlock.SetHeader(kBlockContentLength, 0L);
                newBlock.SetHeader(kNextBlockId, 0L);
                newBlock.SetHeader(kPreviousBlockId, 0L);
                newBlock.SetHeader(kRecordLength, 0L);
                newBlock.SetHeader(kIsDeleted, 0L);
            }

            return newBlock;
        }

        bool TryFindFreeBlock(out uint blockId)
        {
            blockId = 0;
            IBlock lastBlock, secondLastBlock;

            GetSpaceTrackingBlock(out lastBlock, out secondLastBlock);

            using (lastBlock)
            using (secondLastBlock)
            {
                // Go to previous block if current one is empty
                long currentBlockContentLength = lastBlock.GetHeader(kBlockContentLength);

                if (currentBlockContentLength == 0)
                {
                    if (secondLastBlock == null)
                    { return false; }

                    // Dequeue an uint from previous block, then mark current block as free
                    blockId = ReadUInt32FromTrailingContent(secondLastBlock);

                    // Back off 4 bytes before calling AppendUInt32ToContent
                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) - 4);
                    AppendUInt32ToContent(secondLastBlock, lastBlock.Id);

                    // Forward 4 bytes, as an uint32 has been written
                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) + 4);
                    secondLastBlock.SetHeader(kNextBlockId, 0);
                    lastBlock.SetHeader(kPreviousBlockId, 0);

                    return true;
                }
                else
                {
                    // If this block is not empty then dequeue an UInt32 from it
                    blockId = ReadUInt32FromTrailingContent(lastBlock);
                    lastBlock.SetHeader(kBlockContentLength, currentBlockContentLength - 4);

                    return true;
                }
            }
        }

        void AppendUInt32ToContent(IBlock block, uint value)
        {
            long contentLength = block.GetHeader(kBlockContentLength);

            if ((contentLength % 4) != 0)
            { throw new DataMisalignedException("Block content length not %4: " + contentLength); }

            block.Write(source: LittleEndianByteOrder.GetBytes(value), sourceOffset: 0, destinationOffset: (int)contentLength, count: 4);
        }

        uint ReadUInt32FromTrailingContent(IBlock block)
        {
            byte[] buffer = new byte[4];
            long contentLength = block.GetHeader(kBlockContentLength);

            if ((contentLength % 4) != 0)
            { throw new DataMisalignedException("Block content length not %4: " + contentLength); }

            if (contentLength == 0)
            { throw new Exception("Trying to dequeue UInt32 from an empty block."); }

            block.Read(destination: buffer, destinationOffset: 0, sourceOffset: (int)contentLength - 4, count: 4);

            return LittleEndianByteOrder.GetUInt32(buffer);
        }

        void MarkAsFree(uint blockId)
        {
            IBlock lastBlock, secondLastBlock, targetBlock = null;

            GetSpaceTrackingBlock(out lastBlock, out secondLastBlock);

            using (lastBlock)
            using (secondLastBlock)
            {
                try
                {
                    var contentLength = lastBlock.GetHeader(kBlockContentLength);

                    if ((contentLength + 4) <= storage.BlockContentSize)
                    { 
                        // Append a number if there's space left
                        targetBlock = lastBlock; 
                    }
                    else
                    {
                        // Allocate new FRESH (!) block if not
                        targetBlock = storage.CreateNew();

                        targetBlock.SetHeader(kPreviousBlockId, lastBlock.Id);
                        lastBlock.SetHeader(kNextBlockId, targetBlock.Id);

                        contentLength = 0;
                    }

                    AppendUInt32ToContent(targetBlock, blockId);

                    targetBlock.SetHeader(kBlockContentLength, contentLength + 4);
                }
                finally
                {
                    if (targetBlock != null)
                    { targetBlock.Dispose(); }
                }
            }
        }

        /// <summary>
        /// Get the last 2 blocks from the free space tracking record
        /// </summary>
        void GetSpaceTrackingBlock(out IBlock lastBlock, out IBlock secondLastBlock)
        {
            lastBlock = secondLastBlock = null;

            // Find all record 0's blocks
            var blocks = FindBlocks(0);

            try
            {
                if (blocks == null || blocks.Count() == 0)
                { throw new Exception("Failed to find blocks of record 0."); }

                lastBlock = blocks[blocks.Count - 1];

                if (blocks.Count > 1)
                { secondLastBlock = blocks[blocks.Count - 2]; }
            }
            finally
            {
                if (blocks != null)
                {
                    foreach (var block in blocks)
                    {
                        if ((lastBlock == null || lastBlock != block) && (secondLastBlock == null || secondLastBlock != block))
                        { block.Dispose(); }
                    }
                }
            }
        }
        #endregion Methods (private)
    }
}