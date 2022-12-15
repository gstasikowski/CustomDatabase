using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class TreeIntSerializer : ISerializer<uint>
    {
        #region Properties
        public bool IsFixedSize
        {
            get { return true; }
        }

        public int Length
        {
            get { return 4; }
        }
        #endregion Properties

        #region Methods(public)
        public uint Deserialize(byte[] buffer, int offset, int length)
        {
            if (length != 4)
            {
                throw new ArgumentException(CommonResources.GetErrorMessage("InvalidLength") + length);
            }

            return BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: offset);
        }

        public byte[] Serialize(uint value)
        {
            return LittleEndianByteOrder.GetBytes(value);
        }
        #endregion Methods(public)
    }
}