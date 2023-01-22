using CustomDatabase.Interfaces;
using CustomDatabase.Helpers;

namespace CustomDatabase.Logic
{
    public class Block : IBlock
    {
        #region Variables
        private readonly byte[] _firstSector;
        private readonly long?[] _cachedHeaderValue = new long?[5];
        private readonly Stream _stream;
        private readonly BlockStorage _storage;
        private readonly uint _id;

        private bool _isFirstSectorDirty = false;
        private bool _isDisposed = false;

        public event EventHandler Disposed;
        #endregion Variables
        
        #region Properties
        public uint Id
        {
            get { return _id; }
        }
        #endregion Properties

        #region Constructor
        public Block(BlockStorage storage, uint id, byte[] firstSector, Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream");
            }

            if (firstSector == null)
            {
                throw new ArgumentNullException("firstSector");
            }

            if (firstSector.Length != storage.DiskSectorSize)
            {
                throw new ArgumentException(
                    CommonResources.GetErrorMessage("WrongFirstSectorLength") + storage.DiskSectorSize
                );
            }

            _storage = storage;
            _id = id;
            _firstSector = firstSector;
            _stream = stream;
        }
        #endregion Constructor

        #region Methods (public)
        public long GetHeader(int field)
        {
            ValidateBlock(field); 

            if (field >= (_storage.BlockHeaderSize / 8))
            {
                throw new ArgumentException(
                    CommonResources.GetErrorMessage("InvalidField") + field
                );
            }
            

            // Check (and return) if alread in cache
            if (field < _cachedHeaderValue.Length)
            {
                if (_cachedHeaderValue[field] == null)
                { 
                    _cachedHeaderValue[field] = BufferHelper.ReadBufferInt64(
                        buffer: _firstSector,
                        bufferOffset: field * 8
                    );
                }

                return (long)_cachedHeaderValue[field];
            }
            else
            {
                return BufferHelper.ReadBufferInt64(buffer: _firstSector, bufferOffset: field * 8);
            }
        }

        public void SetHeader(int field, long value)
        {
            ValidateBlock(field);

            // Update cache if this field is cached
            if (field < _cachedHeaderValue.Length)
            {
                _cachedHeaderValue[field] = value;
            }

            // Write to cache buffer
            BufferHelper.WriteBuffer(value: value, buffer: _firstSector, bufferOffset: field * 8);
            _isFirstSectorDirty = true;
        }

        public void Read(byte[] destination, int destinationOffset, int sourceOffset, int count)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Block");
            }

            // Validate argument
            if (false == ((count >= 0) && ((count + sourceOffset) <= _storage.BlockContentSize)))
            {
                throw new ArgumentOutOfRangeException(
                    paramName: CommonResources.GetErrorMessage("SourceOutOfBounds") + count,
                    message: "count"
                );
            }

            if (false == ((count + destinationOffset) <= destination.Length))
            {
                throw new ArgumentOutOfRangeException(
                    paramName: CommonResources.GetErrorMessage("DestinationOutOfBounds") + count,
                    message: "count"
                );
            }

            // If part of the remaining data belongs to the firstSector buffer
            // start by copying from firstSector
            int dataCopied = 0;
            bool copyFromFirstSector = (_storage.BlockHeaderSize + sourceOffset) < _storage.DiskSectorSize;

            if (copyFromFirstSector)
            {
                int toCopy = Math.Min(
                    val1: _storage.DiskSectorSize - _storage.BlockHeaderSize - sourceOffset,
                    val2: count
                );

                Buffer.BlockCopy(
                    src: _firstSector,
                    srcOffset: _storage.BlockHeaderSize + sourceOffset,
                    dst: destination,
                    dstOffset: destinationOffset,
                    count: toCopy
                );

                dataCopied += toCopy;
            }

            // Move stream to the correct position if there's more data to be copied
            if (dataCopied < count)
            {
                if (copyFromFirstSector)
                {
                    _stream.Position = (Id * _storage.BlockSize) + _storage.DiskSectorSize;
                }
                else
                {
                    _stream.Position = (Id * _storage.BlockSize) + _storage.BlockHeaderSize * sourceOffset;
                }
            }

            // Start copying until all done
            while (dataCopied < count)
            {
                int bytesToRead = Math.Min(val1: _storage.DiskSectorSize, val2: count - dataCopied);
                int thisRead = _stream.Read(
                    buffer: destination,
                    offset: destinationOffset + dataCopied,
                    count: bytesToRead
                );

                if (thisRead == 0)
                {
                    throw new EndOfStreamException();
                }

                dataCopied += thisRead;
            }
        }

        public void Write(byte[] source, int sourceOffset, int destinationOffset, int count)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Block");
            }

            // Validate argument
            if (false == ((destinationOffset >= 0) && ((count + destinationOffset) <= _storage.BlockContentSize)))
            {
                throw new ArgumentOutOfRangeException(
                    paramName: CommonResources.GetErrorMessage("DestinationOutOfBounds") + count,
                    message: "count"
                );
            }

            if (false == ((count + sourceOffset) <= source.Length))
            {
                throw new ArgumentOutOfRangeException(
                    paramName: CommonResources.GetErrorMessage("SourceOutOfBounds") + count,
                    message: "count"
                );
            }

            // Write bytes that belong to the firstSector
            if ((_storage.BlockHeaderSize + destinationOffset) < _storage.DiskSectorSize)
            {
                int thisWrite = Math.Min(
                    val1: count,
                    val2: _storage.DiskSectorSize - _storage.BlockHeaderSize - destinationOffset
                );
                
                Buffer.BlockCopy(
                    src: source,
                    srcOffset: sourceOffset,
                    dst: _firstSector,
                    dstOffset: _storage.BlockHeaderSize + destinationOffset,
                    count: thisWrite
                );

                _isFirstSectorDirty = true;
            }

            // Write bytes that don't belong to the firstSector
            if ((_storage.BlockHeaderSize + destinationOffset + count) > _storage.DiskSectorSize)
            {
                // Move stream to correct position in prep for writing
                _stream.Position = (Id * _storage.BlockSize) + Math.Max(
                    val1: _storage.DiskSectorSize,
                    val2: _storage.BlockHeaderSize + destinationOffset
                );

                // Exclude bytes already written in firstSector
                int firstSectorData = _storage.DiskSectorSize - (_storage.BlockHeaderSize + destinationOffset);
                
                if (firstSectorData > 0)
                {
                    destinationOffset += firstSectorData;
                    sourceOffset += firstSectorData;
                    count -= firstSectorData;
                }

                // Start writing until all done
                int written = 0;
                while (written < count)
                {
                    int bytesToWrite = Math.Min(val1: 4096, val2: count - written);
                    _stream.Write(buffer: source, offset: sourceOffset + written, count: bytesToWrite);
                    _stream.Flush();
                    written += bytesToWrite;
                }
            }
        }

        public override string ToString()
        {
            return string.Format(
                "[Block: ID={0}, ContentLength={1}, Prev={2}, Next={3}]",
                Id,
                GetHeader(2),
                GetHeader(3),
                GetHeader(0)
            );
        }
        #endregion Methods (public)

        #region Methods (protected)
        protected virtual void OnDisposed(EventArgs e)
        {
            if (Disposed != null)
            {
                Disposed(this, e);
            }
        }
        #endregion Methods (protected)

        #region Methods (private)
        private void ValidateBlock(int field)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Block");
            }

            // Validate field number
            if (field < 0)
            {
                throw new IndexOutOfRangeException();
            }
        }
        #endregion Methods (private)

        #region Dispose
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual void Dispose(bool disposing)
        {
            if (disposing && !_isDisposed)
            {
                _isDisposed = true;

                if (_isFirstSectorDirty)
                {
                    _stream.Position = Id * _storage.BlockSize;
                    _stream.Write(buffer: _firstSector, offset: 0, count: 4096);
                    _stream.Flush();
                    _isFirstSectorDirty = false;
                }

                OnDisposed(EventArgs.Empty);
            }
        }

        ~Block()
        {
            Dispose(false);
        }
        #endregion Dispose
    }
}