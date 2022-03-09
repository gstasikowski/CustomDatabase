using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;

namespace TestApp.Logic
{
    public class StringIntSerializer : ISerializer<Tuple<string, int>>
    {
        #region Getters
        public bool IsFixedSize
        {
            get { return false; }
        }

        public int Length
        {
            get { throw new InvalidOperationException(); }
        }
        #endregion

        #region Methods (public)
        public Tuple<string, int> Deserialize(byte[] buffer, int offset, int length)
        {
            var stringLength = BufferHelper.ReadBufferInt32(buffer, offset);
            
            if (stringLength < 0 || stringLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + stringLength); }

            var stringValue = System.Text.Encoding.UTF8.GetString(buffer, offset + 4, stringLength);
            var intValue = BufferHelper.ReadBufferInt32(buffer, offset + 4 + stringLength);

            return new Tuple<string, int>(stringValue, intValue);
        }

        public byte[] Serialize(Tuple<string, int> value)
        {
            var stringBytes = System.Text.Encoding.UTF8.GetBytes(value.Item1);
            var data = new byte[4 + stringBytes.Length + 4]; // length of the string + content of string + int value

            BufferHelper.WriteBuffer((int)stringBytes.Length, data, 0);
            Buffer.BlockCopy(src: stringBytes, srcOffset: 0, dst: data, dstOffset: 4, count: stringBytes.Length);
            BufferHelper.WriteBuffer((int)value.Item2, data, 4 + stringBytes.Length);

            return data;
        }
        #endregion
    }
}
