using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;

namespace TestApp.Logic
{
    class StringSerializer : ISerializer<Tuple<string, string>>
    {
        #region Properties
        public bool IsFixedSize
        {
            get { return false; }
        }

        public int Length
        {
            get { throw new InvalidOperationException(); }
        }
        #endregion Properties

        #region Methods (public)
        public Tuple<string, string> Deserialize(byte[] buffer, int offset, int length)
        {
            var stringLength = BufferHelper.ReadBufferInt32(buffer, offset);

            if (stringLength < 0 || stringLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + stringLength); }

            var stringValue = System.Text.Encoding.UTF8.GetString(buffer, offset + 4, stringLength);
            var stringLength2 = BufferHelper.ReadBufferInt32(buffer, offset + 4 + stringLength);

            if (stringLength2 < 0 || stringLength2 > (16 * 1024))
            { throw new Exception("Invalid string length: " + stringLength2); }

            var stringValue2 = System.Text.Encoding.UTF8.GetString(buffer, offset + 4, stringLength2);

            return new Tuple<string, string>(stringValue, stringValue2);
        }

        public byte[] Serialize(Tuple<string, string> value)
        {
            var stringBytes = System.Text.Encoding.UTF8.GetBytes(value.Item1);
            var stringBytes2 = System.Text.Encoding.UTF8.GetBytes(value.Item2);
            var data = new byte[4 + stringBytes.Length + 4 + stringBytes2.Length]; // length of the string + content of string + again for second string

            BufferHelper.WriteBuffer((int)stringBytes.Length, data, 0);
            Buffer.BlockCopy(src: stringBytes, srcOffset: 0, dst: data, dstOffset: 4, count: stringBytes.Length);
            BufferHelper.WriteBuffer((int)stringBytes2.Length, data, 4 + stringBytes.Length);
            Buffer.BlockCopy(src: stringBytes2, srcOffset: 0, dst: data, dstOffset: 4 + stringBytes.Length + 4, count: stringBytes2.Length);

            return data;
        }
        #endregion Methods (public)
    }
}
