using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;

namespace TestApp.Logic
{
    public class GuidSerializer : ISerializer<Guid>
    {
        #region Getters
        public bool IsFixedSize
        {
            get { return true; }
        }

        public int Length
        {
            get { return 16; }
        }
        #endregion

        #region Methods (public)
        public Guid Deserialize(byte[] buffer, int offset, int length)
        {
            if (length != 16)
            { throw new ArgumentException("length"); }

            return BufferHelper.ReadBufferGuid(buffer, offset);
        }

        public byte[] Serialize(Guid value)
        {
            return value.ToByteArray();
        }
        #endregion
    }
}
