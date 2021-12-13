using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;

namespace CustomDatabase.Logic
{
    public class BlockStorage : IBlockStorage
    {
        #region Variables
        readonly Stream stream;
        readonly int blockContentSize;
        readonly int blockHeaderSize;
        readonly int blockSize;
        readonly int unitOfWork;
        readonly Dictionary<uint, Block> blocks = new Dictionary<uint, Block>();
        #endregion Variables

        #region Getters
        public int BlockContentSize
        {
            get { return blockContentSize; }
        }

        public int BlockHeaderSize
        {
            get { return blockHeaderSize; }
        }

        public int BlockSize
        {
            get { return blockSize; }
        }

        public int DiskSectorSize
        {
            get { return unitOfWork; }
        }
        #endregion Methods

        #region Constructors
        public BlockStorage(Stream storage, int blockSize = 40960, int blockHeaderSize = 48)
        {
            if (storage == null)
            { throw new ArgumentNullException("storage"); }

            if (blockHeaderSize >= blockSize)
            { throw new ArgumentException("blockHeaderSize can't be >= to blockSize"); }

            if (blockSize < 128)
            { throw new ArgumentException("blockSize too small"); }

            this.blockContentSize = blockSize - blockHeaderSize;
            this.blockHeaderSize = blockHeaderSize;
            this.blockSize = blockSize;
            this.unitOfWork = (blockSize >= 4096) ? 4096 : 128;
            this.stream = storage;
        }
        #endregion Constructors

        #region Methods (public)
        public IBlock CreateNew()
        {
            if ((stream.Length % blockSize) != 0)
            { throw new DataMisalignedException("Unexpected length of the stream: " + stream.Length); }

            // Calculate new block ID
            uint blockID = (uint)Math.Ceiling((double)stream.Length / (double)blockSize);

            // Extend length of underlying stream
            stream.SetLength((long)((blockID * blockSize) + blockSize));
            stream.Flush();

            // Return new block
            Block newBlock = new Block(this, blockID, new byte[DiskSectorSize], stream);
            OnBlockInitialized(newBlock);
            
            return newBlock;
        }

        public IBlock Find(uint blockID)
        {
            // Search from initialized block
            if (blocks.ContainsKey(blockID))
            { return blocks[blockID]; }

            // Move to the initialized block or return NULL if it doesn't exist
            var blockPosition = blockID * blockSize;
            if ((blockPosition + blockSize) > stream.Length)
            { return null; }

            // Read the first 4KB of the block to construct a block from it
            var firstSector = new byte[DiskSectorSize];
            stream.Position = blockID * blockSize;
            stream.Read(firstSector, 0, DiskSectorSize);

            var newBlock = new Block(this, blockID, firstSector, stream);
            OnBlockInitialized(newBlock);

            return newBlock;
        }
        #endregion Methods (public)

        #region Methods (protected)
        protected virtual void OnBlockInitialized(Block block)
        {
            // Keep reference to initialized block
            blocks[block.ID] = block;

            // Remove from memory when block is disposed
            block.Disposed += HandleBlockDisposed;
        }

        protected virtual void HandleBlockDisposed(object sender, EventArgs e)
        {
            // Stop listening to the block
            var block = (Block)sender;
            block.Disposed -= HandleBlockDisposed;

            // Remove block from memory
            blocks.Remove(block.ID);
        }
        #endregion Methods (protected)
    }
}
