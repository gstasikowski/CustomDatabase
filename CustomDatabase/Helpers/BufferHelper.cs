namespace CustomDatabase.Helpers
{
    /// <summary>
    /// Helper class containing static methods for reading/writing numeric values
    /// into/from a byte array in little endian byte order.
    /// </summary>
    public static class BufferHelper
    {
        #region Readers
        public static Guid ReadBufferGuid(byte[] buffer, int bufferOffset)
        {
            byte[] guidBuffer = new byte[16];
            Buffer.BlockCopy(
                src: buffer,
                srcOffset: bufferOffset,
                dst: guidBuffer,
                dstOffset: 0,
                count: 16
            );
            
            return new Guid(guidBuffer);
        }

        public static uint ReadBufferUInt32(byte[] buffer, int bufferOffset)
        {
            byte[] uintBuffer = new byte[4];
            Buffer.BlockCopy(
                src: buffer,
                srcOffset: bufferOffset,
                dst: uintBuffer,
                dstOffset: 0,
                count: 4
            );

            return LittleEndianByteOrder.GetUInt32(uintBuffer);
        }

        public static int ReadBufferInt32(byte[] buffer, int bufferOffset)
        {
            byte[] intBuffer = new byte[4];
            Buffer.BlockCopy(
                src: buffer, 
                srcOffset: bufferOffset,
                dst: intBuffer,
                dstOffset: 0,
                count: 4
            );

            return LittleEndianByteOrder.GetInt32(intBuffer);
        }

        public static long ReadBufferInt64(byte[] buffer, int bufferOffset)
		{
			byte[] longBuffer = new byte[8];
			Buffer.BlockCopy(
                src: buffer,
                srcOffset: bufferOffset,
                dst: longBuffer,
                dstOffset: 0,
                count: 8
            );

			return LittleEndianByteOrder.GetInt64(longBuffer);
		}

		public static double ReadBufferDouble(byte[] buffer, int bufferOffset)
		{
			byte[] doubleBuffer = new byte[8];
			Buffer.BlockCopy(
                src: buffer,
                srcOffset: bufferOffset,
                dst: doubleBuffer,
                dstOffset: 0,
                count: 8
            );

			return LittleEndianByteOrder.GetDouble(doubleBuffer);
		}
        #endregion Readers

        #region Writers
        public static void WriteBuffer(double value, byte[] buffer, int bufferOffset)
		{
			Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes(value),
                srcOffset: 0,
                dst: buffer,
                dstOffset: bufferOffset,
                count: 8
            );
		}

		public static void WriteBuffer(uint value, byte[] buffer, int bufferOffset)
		{
			Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes(value), 
                srcOffset: 0, 
                dst: buffer, 
                dstOffset: bufferOffset, 
                count: 4
            );
		}

		public static void WriteBuffer(long value, byte[] buffer, int bufferOffset)
		{
			Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes(value), 
                srcOffset: 0, 
                dst: buffer, 
                dstOffset: bufferOffset, 
                count: 8
            );
		}

		public static void WriteBuffer(int value, byte[] buffer, int bufferOffset)
		{
			Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)value), 
                srcOffset: 0, 
                dst: buffer, 
                dstOffset: bufferOffset, 
                count: 4
            );
		}

		public static void WriteBuffer(Guid value, byte[] buffer, int bufferOffset)
		{
			Buffer.BlockCopy(
                src: value.ToByteArray(), 
                srcOffset: 0, 
                dst: buffer, 
                dstOffset: bufferOffset, 
                count: 16
            );
		}
        #endregion Writers
    }
}