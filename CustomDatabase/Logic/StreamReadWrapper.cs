namespace CustomDatabase.Logic
{
    /// <summary>
    /// Wrapped around a stream, read only.
    /// Allows client to limit a stream to a particular length.
    /// </summary>
    public class StreamReadWrapper : Stream
    {
        #region Variables
        private readonly Stream _parent;
        private long _readLimit;
        private long _position = 0;
        #endregion Variables

        #region Properties
        public override long Position 
        {
            get { return _position; }
            set { throw new NotImplementedException(); }
        }

        public override long Length
        {
            get { return _readLimit; }
        }

        public override bool CanRead
        {
            get { return _parent.CanRead; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }
        #endregion Properties

        #region Constructor
        public StreamReadWrapper(Stream target, long readLimit)
        {
            _parent = target;
            _readLimit = readLimit;
        }
        #endregion Constructor

        #region Methods (public)
        public override void Flush()
        {
            // dummy method
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if ((_readLimit - _position) == 0)
            {
                return 0;
            }

            int read = _parent.Read(
                buffer: buffer,
                offset: offset,
                count: (int)Math.Min(val1: count, val2: _readLimit - _position)
            );

            _position += read;

            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
        #endregion Methods (public)
    }
}