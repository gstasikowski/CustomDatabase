using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace TestApp.Logic
{
    public class GuidSerializer : ISerializer<Guid>
    {
        #region Properties
        public bool IsFixedSize
        {
            get { return true; }
        }

        public int Length
        {
            get { return 16; }
        }
        #endregion Properties

        #region Methods (public)
        public Guid Deserialize(byte[] buffer, int offset, int length)
        {
            if (length != 16)
            {
                throw new ArgumentException("length");
            }

            return BufferHelper.ReadBufferGuid(buffer: buffer, bufferOffset: offset);
        }

        public byte[] Serialize(Guid value)
        {
            return value.ToByteArray();
        }
        #endregion Methods (public)
    }
}