using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace TestApp.Logic
{
    public class StringIntSerializer : ISerializer<Tuple<string, int>>
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
        public Tuple<string, int> Deserialize(byte[] buffer, int offset, int length)
        {
            int stringLength = BufferHelper.ReadBufferInt32(buffer: buffer, bufferOffset: offset);
            
            if (stringLength < 0 || stringLength > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + stringLength
                );
            }

            string stringValue = System.Text.Encoding.UTF8.GetString(
                bytes: buffer,
                index: offset + 4,
                count: stringLength
            );
            int intValue = BufferHelper.ReadBufferInt32(buffer: buffer, bufferOffset: offset + 4 + stringLength);

            return new Tuple<string, int>(item1: stringValue, item2: intValue);
        }

        public byte[] Serialize(Tuple<string, int> value)
        {
            byte[] stringBytes = System.Text.Encoding.UTF8.GetBytes(value.Item1);
            byte[] data = new byte[4 + stringBytes.Length + 4]; // length of the string + content of string + int value

            BufferHelper.WriteBuffer(value: (int)stringBytes.Length, buffer: data, bufferOffset: 0);
            Buffer.BlockCopy(src: stringBytes, srcOffset: 0, dst: data, dstOffset: 4, count: stringBytes.Length);
            BufferHelper.WriteBuffer(value: (int)value.Item2, buffer: data, bufferOffset: 4 + stringBytes.Length);

            return data;
        }
        #endregion Methods (public)
    }
}