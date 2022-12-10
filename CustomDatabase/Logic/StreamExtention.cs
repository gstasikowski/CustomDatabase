using CustomDatabase.Helpers;

namespace CustomDatabase.Logic
{
    public static class StreamExtension
    {
        #region Methods (public)
        /// <summary>
        /// Treat given upcoming data as stream & return as such.
        /// </summary>
        public static StreamReadWrapper ExpectStream(this Stream target, long length)
        {
            return new StreamReadWrapper(target, length);
        }

        /// <summary>
        /// Write all buffer into stream.
        /// </summary>
        public static void Write(this Stream stream, byte[] buffer)
        {
            stream.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Read until given buffer is filled or end of the stream is reached.
        /// </summary>
        public static int Read(this Stream src, byte[] buffer)
        {
            int filled = 0;
            int lastRead = 0;

            while (filled < buffer.Length)
            {
                lastRead = src.Read(buffer, filled, buffer.Length - filled);
                filled += lastRead;

                if (lastRead == 0)
                { break; }
            }

            return filled;
        }

        /// <summary>
        /// Read until given buffer is filled or end of the stream is reached (async).
        /// </summary>
        public static async Task<int> ReadAsync(this Stream source, byte[] buffer)
        {
            int filled = 0;
            int lastRead = 0;

            while (filled < buffer.Length)
            {
                lastRead = await source.ReadAsync(buffer, filled, buffer.Length - filled);
                filled += lastRead;

                if (lastRead == 0)
                { break; }
            }

            return filled;
        }

        /// <summary>
        /// Similar to Stream.CopyTo but with option to inject a delegate
        /// to receive feedback (for loading bars etc).
        /// </summary>
        public static void CopyTo(this Stream source,
            Stream destination,
            int bufferSize = 4096,
            Func<long, bool> feedback = null,
            long maxLength = 0)
        {
            var buffer = new byte[bufferSize];
            var totalRead = 0L;

            while (totalRead < maxLength)
            {
                int bytesToRead = (int)Math.Min(maxLength - totalRead, buffer.Length);
                int thisRead = source.Read(buffer, 0, bytesToRead);

                if (thisRead == 0)
                { throw new EndOfStreamException(); }

                totalRead += thisRead;

                destination.Write(buffer, 0, thisRead);
                destination.Flush();

                // Call feedback, if "false" then stop copying
                if (feedback != null && feedback(totalRead) == false)
                { return; }
            }
        }

        /// <summary>
        /// Ditto but async.
        /// </summary>
        public static async Task CopyToAsync(this Stream source,
            Stream destination,
            int bufferSize = 4096,
            Func<long, bool> feedback = null,
            long maxLength = 0)
        {
            var buffer = new byte[bufferSize];
            var totalRead = 0L;

            while (totalRead < maxLength)
            {
                int bytesToRead = (int)Math.Min(maxLength - totalRead, buffer.Length);
                int thisRead = await source.ReadAsync(buffer, 0, bytesToRead);

                if (thisRead == 0)
                { throw new EndOfStreamException(); }

                totalRead += thisRead;

                destination.Write(buffer, 0, thisRead);
                destination.Flush();

                // Call feedback, if "false" then stop copying
                if (feedback != null && feedback(totalRead) == false)
                { return; }
            }
        }
        #endregion Methods (public)

        #region Methods (Expect_)
        /// <summary>
        /// Expect the upcoming 4 bytes to be a float, read and return it.
        /// </summary>
        public static float ExpectFloat(this Stream target)
        {
            var buff = new byte[4];

            if (target.Read(buff) == 4)
            { return LittleEndianByteOrder.GetSingle(buff); }
            else
            { throw new EndOfStreamException(); }
        }

        /// <summary>
        /// Expect the upcoming 4 bytes to be a int32, read and return it.
        /// </summary>
        public static int ExpectInt32(this Stream target)
        {
            var buff = new byte[4];

            if (target.Read(buff) == 4)
            { return LittleEndianByteOrder.GetInt32(buff); }
            else
            { throw new EndOfStreamException(); }
        }

        /// <summary>
        /// Expect the upcoming 4 bytes to be an uint32, read and return it.
        /// </summary>
        public static uint ExpectUInt32(this Stream target)
        {
            var buff = new byte[4];

            if (target.Read(buff) == 4)
            { return LittleEndianByteOrder.GetUInt32(buff); }
            else
            { throw new EndOfStreamException(); }
        }

        /// <summary>
        /// Expect the upcoming 8 bytes to be a int64, read and return it.
        /// </summary>
        public static long ExpectInt64(this Stream target)
        {
            var buff = new byte[8];

            if (target.Read(buff) == 8)
            { return LittleEndianByteOrder.GetInt64(buff); }
            else
            { throw new EndOfStreamException(); }
        }

        /// <summary>
        /// Expect the upcoming 8 bytes to be a double, read and return it.
        /// </summary>
        public static double ExpectDouble(this Stream target)
        {
            var buff = new byte[8];

            if (target.Read(buff) == 8)
            { return LittleEndianByteOrder.GetDouble(buff); }
            else
            { throw new EndOfStreamException(); }
        }

        /// <summary>
        /// Expect the upcoming byte to be a bool, read and return it.
        /// </summary>
        public static bool ExpectBool(this Stream target)
        {
            return Convert.ToBoolean(target.ReadByte());
        }

        /// <summary>
        /// Expect the upcoming 16 bytes to be a guid, read and return it.
        /// </summary>
        public static Guid ExpectGuid(this Stream target)
        {
            var buff = new byte[16];

            if (target.Read(buff) == 16)
            { return new Guid(buff); }
            else
            { throw new EndOfStreamException(); }
        }
        #endregion Methods (Expect_)
    }
}