using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class TreeLongSerializer : ISerializer<long>
    {
        #region Properties
        public bool IsFixedSize
        {
            get { return true; }
        }

        public int Length
        {
            get { return 8; }
        }
        #endregion Properties

        #region Methods (public)
        public long Deserialize(byte[] buffer, int offset, int length)
        {
            if (length != 8)
            {
                throw new ArgumentException(CommonResources.GetErrorMessage("InvalidLength") + length);
            }

            return BufferHelper.ReadBufferInt64(buffer: buffer, bufferOffset: offset);
        }

        public byte[] Serialize(long value)
        {
            return LittleEndianByteOrder.GetBytes(value);
        }
        #endregion Methods (public)
    }
}