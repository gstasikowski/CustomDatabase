using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;

namespace CustomDatabase.Logic.Tree
{
    public class TreeUIntSerializer : ISerializer<uint>
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
            { throw new ArgumentException("Invalid length: " + length); }

            return BufferHelper.ReadBufferUInt32(buffer, offset);
        }

        public byte[] Serialize(uint value)
        {
            return LittleEndianByteOrder.GetBytes(value);
        }
        #endregion Methods(public)
    }
}
