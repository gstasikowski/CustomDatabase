using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

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
            int stringLength = BufferHelper.ReadBufferInt32(buffer, offset);

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
            int stringLength2 = BufferHelper.ReadBufferInt32(
                buffer: buffer,
                bufferOffset: offset + 4 + stringLength
            );

            if (stringLength2 < 0 || stringLength2 > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + stringLength2
                );
            }

            string stringValue2 = System.Text.Encoding.UTF8.GetString(
                bytes: buffer,
                index: offset + 4,
                count: stringLength2
            );

            return new Tuple<string, string>(item1: stringValue, item2: stringValue2);
        }

        public byte[] Serialize(Tuple<string, string> value)
        {
            byte[] stringBytes = System.Text.Encoding.UTF8.GetBytes(value.Item1);
            byte[] stringBytes2 = System.Text.Encoding.UTF8.GetBytes(value.Item2);
            // length of the string + content of string + again for second string
            byte[] data = new byte[4 + stringBytes.Length + 4 + stringBytes2.Length];

            BufferHelper.WriteBuffer(value: (int)stringBytes.Length, buffer: data, bufferOffset: 0);
            Buffer.BlockCopy(
                src: stringBytes,
                srcOffset: 0,
                dst: data,
                dstOffset: 4,
                count: stringBytes.Length
            );
            BufferHelper.WriteBuffer(
                value: (int)stringBytes2.Length,
                buffer: data,
                bufferOffset: 4 + stringBytes.Length
            );
            Buffer.BlockCopy(
                src: stringBytes2,
                srcOffset: 0,
                dst: data,
                dstOffset: 4 + stringBytes.Length + 4,
                count: stringBytes2.Length
            );

            return data;
        }
        #endregion Methods (public)
    }
}