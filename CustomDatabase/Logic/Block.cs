using CustomDatabase.Interfaces;
using CustomDatabase.Helpers;
using System;
using System.IO;

namespace CustomDatabase.Logic
{
    public class Block : IBlock
    {
        #region Variables
        readonly byte[] firstSector;
        readonly long?[] cachedHeaderValue = new long?[5];
        readonly Stream stream;
        readonly BlockStorage storage;
        readonly uint id;

        bool isFirstSectorDirty = false;
        bool isDisposed = false;

        public event EventHandler Disposed;
        #endregion Variables
        
        #region Properties
        public uint ID
        {
            get { return id; }
        }
        #endregion Properties

        #region Constructor
        public Block(BlockStorage storage, uint id, byte[] firstSector, Stream stream)
        {
            if (stream == null)
            { throw new ArgumentNullException("stream"); }

            if (firstSector == null)
            { throw new ArgumentNullException("firstSector"); }

            if (firstSector.Length != storage.DiskSectorSize)
            { throw new ArgumentException("first sector length must be: " + storage.DiskSectorSize); }

            this.storage = storage;
            this.id = id;
            this.firstSector = firstSector;
            this.stream = stream;
        }
        #endregion Constructor

        #region Methods (public)
        public long GetHeader(int field)
        {
            ValidateBlock(field); 

            if (field >= (storage.BlockHeaderSize/8))
            { throw new ArgumentException("Invalid field: " + field); }
            

            // Check (and return) if alread in cache
            if (field < cachedHeaderValue.Length)
            {
                if (cachedHeaderValue[field] == null)
                { cachedHeaderValue[field] = BufferHelper.ReadBufferInt64(firstSector, field * 8); }

                return (long)cachedHeaderValue[field];
            }
            else
            {
                return BufferHelper.ReadBufferInt64(firstSector, field * 8);
            }
        }

        public void SetHeader(int field, long value)
        {
            ValidateBlock(field);

            // Update cache if this field is cached
            if (field < cachedHeaderValue.Length)
            { cachedHeaderValue[field] = value; }

            // Write to cache buffer
            BufferHelper.WriteBuffer(value, firstSector, field * 8);
            isFirstSectorDirty = true;
        }

        public void Read(byte[] dst, int dstOffset, int srcOffset, int count)
        {
            if (isDisposed)
            { throw new ObjectDisposedException("Block"); }

            // Validate argument
            if (false == ((count >= 0) && ((count + srcOffset) <= storage.BlockContentSize)))
            { throw new ArgumentOutOfRangeException("Requested count is outside of src bounds. Count: " + count, "count"); } // TODO move error handling to a centralized place

            if (false == ((count + dstOffset) <= dst.Length))
            { throw new ArgumentOutOfRangeException("Requested count is outside dest bounds. Count: " + count); }

            // If part of the remaining data belongs to the firstSector buffer
            // start by copying from firstSector
            int dataCopied = 0;
            bool copyFromFirstSector = (storage.BlockHeaderSize + srcOffset) < storage.DiskSectorSize;

            if (copyFromFirstSector)
            {
                int toCopy = Math.Min(storage.DiskSectorSize - storage.BlockHeaderSize - srcOffset, count);

                Buffer.BlockCopy(src: firstSector,
                    srcOffset: storage.BlockHeaderSize + srcOffset,
                    dst: dst,
                    dstOffset: dstOffset,
                    count: toCopy);

                dataCopied += toCopy;
            }

            // Move stream to the correct position if there's more data to be copied
            if (dataCopied < count)
            {
                if (copyFromFirstSector)
                { stream.Position = (ID * storage.BlockSize) + storage.DiskSectorSize; }
                else
                { stream.Position = (ID * storage.BlockSize) + storage.BlockHeaderSize * srcOffset; }
            }

            // Start copying until all done
            while (dataCopied < count)
            {
                int bytesToRead = Math.Min(storage.DiskSectorSize, count - dataCopied);
                int thisRead = stream.Read(dst, dstOffset + dataCopied, bytesToRead);

                if (thisRead == 0)
                { throw new EndOfStreamException(); }

                dataCopied += thisRead;
            }
        }

        public void Write(byte[] src, int srcOffset, int dstOffset, int count)
        {
            if (isDisposed)
            { throw new ObjectDisposedException("Block"); }

            // Validate argument
            if (false == ((dstOffset >= 0) && ((count + dstOffset) <= storage.BlockContentSize)))
            { throw new ArgumentOutOfRangeException("Requested count is outside of dst bounds. Count: " + count, "count"); }

            if (false == ((count + srcOffset) <= src.Length))
            { throw new ArgumentOutOfRangeException("Requested count is outside src bounds. Count: " + count); }

            // Write bytes that belong to the firstSector
            if ((storage.BlockHeaderSize + dstOffset) < storage.DiskSectorSize)
            {
                int thisWrite = Math.Min(count, storage.DiskSectorSize - storage.BlockHeaderSize - dstOffset);
                
                Buffer.BlockCopy(src: src,
                    srcOffset: srcOffset,
                    dst: firstSector,
                    dstOffset: storage.BlockHeaderSize + dstOffset,
                    count: thisWrite);

                isFirstSectorDirty = true;
            }

            // Write bytes that don't belong to the firstSector
            if ((storage.BlockHeaderSize + dstOffset + count) > storage.DiskSectorSize)
            {
                // Move stream to correct position in prep for writing
                this.stream.Position = (ID * storage.BlockSize) + Math.Max(storage.DiskSectorSize, storage.BlockHeaderSize + dstOffset);

                // Exclude bytes already written in firstSector
                int firstSectorData = storage.DiskSectorSize - (storage.BlockHeaderSize + dstOffset);
                
                if (firstSectorData > 0)
                {
                    dstOffset += firstSectorData;
                    srcOffset += firstSectorData;
                    count -= firstSectorData;
                }

                // Start writing until all done
                int written = 0;
                while (written < count)
                {
                    int bytesToWrite = Math.Min(4096, count - written);
                    this.stream.Write(src, srcOffset + written, bytesToWrite);
                    this.stream.Flush();
                    written += bytesToWrite;
                }
            }
        }

        public override string ToString()
        {
            return string.Format("[Block: ID={0}, ContentLength={1}, Prev={2}, Next={3}]",
                ID, GetHeader(2), GetHeader(3), GetHeader(0));
        }
        #endregion Methods (public)

        #region Methods (private)
        private void ValidateBlock(int field)
        {
            if (isDisposed)
            { throw new ObjectDisposedException("Block"); }

            // Validate field number
            if (field < 0)
            { throw new IndexOutOfRangeException(); }
        }
        #endregion Methods (private)

        #region Methods (protected)
        protected virtual void OnDisposed(EventArgs e)
        {
            if (Disposed != null)
            { Disposed(this, e); }
        }
        #endregion Methods (protected)

        #region Dispose
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public virtual void Dispose(bool disposing)
        {
            if (disposing && !isDisposed)
            {
                isDisposed = true;

                if (isFirstSectorDirty)
                {
                    this.stream.Position = ID * storage.BlockSize;
                    this.stream.Write(firstSector, 0, 4096);
                    this.stream.Flush();
                    isFirstSectorDirty = false;
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
