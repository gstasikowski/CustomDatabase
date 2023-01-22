using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class BlockStorage : IBlockStorage
    {
        #region Variables
        private readonly Stream _stream;
        private readonly int _blockContentSize;
        private readonly int _blockHeaderSize;
        private readonly int _blockSize;
        private readonly int _unitOfWork;
        private readonly Dictionary<uint, Block> _blocks = new Dictionary<uint, Block>();
        #endregion Variables

        #region Properties
        public int BlockContentSize
        {
            get { return _blockContentSize; }
        }

        public int BlockHeaderSize
        {
            get { return _blockHeaderSize; }
        }

        public int BlockSize
        {
            get { return _blockSize; }
        }

        public int DiskSectorSize
        {
            get { return _unitOfWork; }
        }
        #endregion Properties

        #region Constructors
        public BlockStorage(Stream storage, int blockSize = 40960, int blockHeaderSize = 48)
        {
            if (storage == null)
            {
                throw new ArgumentNullException("storage");
            }

            if (blockHeaderSize >= blockSize)
            {
                throw new ArgumentException(CommonResources.GetErrorMessage("BlockHeaderSizeTooBig"));
            }

            if (blockSize < 128)
            {
                throw new ArgumentException("blockSize too small");
            }

            _blockContentSize = blockSize - blockHeaderSize;
            _blockHeaderSize = blockHeaderSize;
            _blockSize = blockSize;
            _unitOfWork = (blockSize >= 4096) ? 4096 : 128;
            _stream = storage;
        }
        #endregion Constructors

        #region Methods (public)
        public IBlock CreateNew()
        {
            if ((_stream.Length % _blockSize) != 0)
            {
                throw new DataMisalignedException(
                    CommonResources.GetErrorMessage("UnexpectedStreamLength") + _stream.Length
                );
            }

            // Calculate new block ID
            uint blockId = (uint)Math.Ceiling((double)_stream.Length / (double)_blockSize);

            // Extend length of underlying stream
            _stream.SetLength((long)((blockId * _blockSize) + _blockSize));
            _stream.Flush();

            // Return new block
            Block newBlock = new Block(
                storage: this,
                id: blockId,
                firstSector: new byte[DiskSectorSize],
                stream: _stream
            );
            OnBlockInitialized(newBlock);
            
            return newBlock;
        }

        public IBlock Find(uint blockId)
        {
            // Search from initialized block
            if (_blocks.ContainsKey(blockId))
            {
                return _blocks[blockId];
            }

            // Move to the initialized block or return NULL if it doesn't exist
            long blockPosition = blockId * _blockSize;

            if ((blockPosition + _blockSize) > _stream.Length)
            {
                return null;
            }

            // Read the first 4KB of the block to construct a block from it
            byte[] firstSector = new byte[DiskSectorSize];
            _stream.Position = blockId * _blockSize;
            _stream.Read(buffer: firstSector, offset: 0, count: DiskSectorSize);

            var newBlock = new Block(storage: this, id: blockId, firstSector: firstSector, stream: _stream);
            OnBlockInitialized(newBlock);

            return newBlock;
        }
        #endregion Methods (public)

        #region Methods (protected)
        protected virtual void OnBlockInitialized(Block block)
        {
            // Keep reference to initialized block
            _blocks[block.Id] = block;

            // Remove from memory when block is disposed
            block.Disposed += HandleBlockDisposed;
        }

        protected virtual void HandleBlockDisposed(object sender, EventArgs e)
        {
            // Stop listening to the block
            var block = (Block)sender;
            block.Disposed -= HandleBlockDisposed;

            // Remove block from memory
            _blocks.Remove(block.Id);
        }
        #endregion Methods (protected)
    }
}