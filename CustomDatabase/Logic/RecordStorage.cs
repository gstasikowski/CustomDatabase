using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CustomDatabase.Logic
{
    public class RecordStorage : IRecordStorage
    {
        #region Variables
        readonly IBlockStorage storage;

        const int MaxRecordSize = 4194304; // 4MB
        const int kNextBlockID = 0;
        const int kRecordLength = 1;
        const int kBlockContentLength = 2;
        const int kPreviousBlockID = 3;
        const int kIsDeleted = 4;
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
        public virtual byte[] Find(uint recordID)
        {
            using (var block = storage.Find(recordID))
            {
                if (block == null)
                { return null; }

                // Ignore if block is deleted.
                if (1L == block.GetHeader(kIsDeleted))
                { return null; }

                // Ignore if block is a child.
                if (0L != block.GetHeader(kPreviousBlockID))
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
                    uint nextBlockID;

                    using (currentBlock)
                    {
                        var thisBlockContentLength = currentBlock.GetHeader(kBlockContentLength);

                        if (thisBlockContentLength > storage.BlockContentSize)
                        { throw new Exception("Unexpected block content length: " + thisBlockContentLength); }

                        // Reading all content from current block
                        currentBlock.Read(dst: data, dstOffset: bytesRead, srcOffset: 0, count: (int)thisBlockContentLength);

                        bytesRead += (int)thisBlockContentLength;

                        nextBlockID = (uint)currentBlock.GetHeader(kNextBlockID);

                        if (nextBlockID == 0)
                        { return data; }
                    }

                    currentBlock = this.storage.Find(nextBlockID);

                    if (currentBlock == null)
                    { throw new Exception("Block not found by ID: " + nextBlockID); }
                }
            }
        }

        public virtual uint Create(Func<uint, byte[]> dataGenerator)
        {
            if (dataGenerator == null)
            { throw new ArgumentException(); }

            using (var firstBlock = AllocateBlock())
            {
                uint returnID = firstBlock.ID;

                byte[] data = dataGenerator(returnID);
                int dataWritten = 0;
                int dataToWrite = data.Length;

                firstBlock.SetHeader(kRecordLength, dataToWrite);

                if (dataToWrite == 0)
                { return returnID; }

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
                                nextBlock.SetHeader(kPreviousBlockID, currentBlock.ID);
                                currentBlock.SetHeader(kNextBlockID, nextBlock.ID);
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

                return returnID;
            }
        }

        public virtual uint Create(byte[] data)
        {
            if (data == null)
            { throw new ArgumentException(); }

            return Create(recordID => data);
        }

        public virtual uint Create()
        {
            using (var firstBlock = AllocateBlock())
            { return firstBlock.ID; }
        }

        public virtual void Delete(uint recordID)
        {
            using (var block = storage.Find(recordID))
            {
                IBlock currentBlock = block;

                while (true)
                {
                    IBlock nextBlock = null;

                    using (currentBlock)
                    {
                        MarkAsFree(currentBlock.ID);
                        currentBlock.SetHeader(kIsDeleted, 1L);

                        uint nextBlockID = (uint)currentBlock.GetHeader(kNextBlockID);

                        if (nextBlockID == 0)
                        { break; }
                        else
                        {
                            nextBlock = storage.Find(nextBlockID);

                            if (currentBlock == null)
                            { throw new Exception("Block not found by ID: " + nextBlockID); }
                        }
                    }

                    if (nextBlock != null)
                    { currentBlock = nextBlock; }
                }
            }
        }

        public virtual void Update(uint recordID, byte[] data)
        {
            int written = 0;
            int total = data.Length;
            var blocks = FindBlocks(recordID);
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
                        previousBlock.SetHeader(kNextBlockID, target.ID);
                        target.SetHeader(kPreviousBlockID, previousBlock.ID);
                    }

                    target.Write(src: data, srcOffset: written, dstOffset: 0, count: bytesToWrite);
                    target.SetHeader(kBlockContentLength, bytesToWrite);
                    target.SetHeader(kNextBlockID, 0);

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
                    { MarkAsFree(blocks[i].ID); }
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
        /// <param name="recordID">Record identifier.</param>
        List<IBlock> FindBlocks(uint recordID)
        {
            var blocks = new List<IBlock>();
            bool isSuccess = false;

            try
            {
                uint currentBlockID = recordID;

                do
                {
                    var block = storage.Find(currentBlockID);

                    if (block == null)
                    {
                        // If block #0 was never created try doing it
                        if (currentBlockID == 0)
                        { block = storage.CreateNew(); }
                        else
                        { throw new Exception("Block not found by ID: " + currentBlockID); }
                    }

                    blocks.Add(block);

                    // Ignore the block if it's deleted
                    if (1L == block.GetHeader(kIsDeleted))
                    { throw new Exception("Block not found: " + currentBlockID); }

                    currentBlockID = (uint)block.GetHeader(kNextBlockID);
                } while (currentBlockID != 0);

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
            uint reusableBlockID;
            IBlock newBlock;

            if (!TryFindFreeBlock(out reusableBlockID))
            {
                newBlock = storage.CreateNew();

                if (newBlock == null)
                { throw new Exception("Failed to create new block."); }
            }
            else
            {
                newBlock = storage.Find(reusableBlockID);

                if (newBlock == null)
                { throw new Exception("Block not found by ID: " + reusableBlockID); }

                newBlock.SetHeader(kBlockContentLength, 0L);
                newBlock.SetHeader(kNextBlockID, 0L);
                newBlock.SetHeader(kPreviousBlockID, 0L);
                newBlock.SetHeader(kRecordLength, 0L);
                newBlock.SetHeader(kIsDeleted, 0L);
            }

            return newBlock;
        }

        bool TryFindFreeBlock(out uint blockID)
        {
            blockID = 0;
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
                    blockID = ReadUInt32FromTrailingContent(secondLastBlock);

                    // Back off 4 bytes before calling AppendUInt32ToContent
                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) - 4);
                    AppendUInt32ToContent(secondLastBlock, lastBlock.ID);

                    // Forward 4 bytes, as an uint32 has been written
                    secondLastBlock.SetHeader(kBlockContentLength, secondLastBlock.GetHeader(kBlockContentLength) + 4);
                    secondLastBlock.SetHeader(kNextBlockID, 0);
                    lastBlock.SetHeader(kPreviousBlockID, 0);

                    return true;
                }
                else
                {
                    // If this block is not empty then dequeue an UInt32 from it
                    blockID = ReadUInt32FromTrailingContent(lastBlock);
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

            block.Write(src: LittleEndianByteOrder.GetBytes(value), srcOffset: 0, dstOffset: (int)contentLength, count: 4);
        }

        uint ReadUInt32FromTrailingContent(IBlock block)
        {
            byte[] buffer = new byte[4];
            long contentLength = block.GetHeader(kBlockContentLength);

            if ((contentLength % 4) != 0)
            { throw new DataMisalignedException("Block content length not %4: " + contentLength); }

            if (contentLength == 0)
            { throw new Exception("Trying to dequeue UInt32 from an empty block."); }

            block.Read(dst: buffer, dstOffset: 0, srcOffset: (int)contentLength - 4, count: 4);

            return LittleEndianByteOrder.GetUInt32(buffer);
        }

        void MarkAsFree(uint blockID)
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

                        targetBlock.SetHeader(kPreviousBlockID, lastBlock.ID);
                        lastBlock.SetHeader(kNextBlockID, targetBlock.ID);

                        contentLength = 0;
                    }

                    AppendUInt32ToContent(targetBlock, blockID);

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
