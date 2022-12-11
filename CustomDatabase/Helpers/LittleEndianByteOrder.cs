namespace CustomDatabase.Helpers
{
    /// <summary>
	/// Helper class containing static methods to read/write
	/// numeric data in little endian byte order
	/// </summary>
    public static class LittleEndianByteOrder
    {
		public static byte[] GetBytes(int value)
		{
			byte[] bytes = BitConverter.GetBytes(value);

			if (false == BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes);
			}

			return bytes;
		}

		public static byte[] GetBytes(long value)
		{
			byte[] bytes = BitConverter.GetBytes(value);

			if (false == BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes);
			}

			return bytes;
		}

		public static byte[] GetBytes(uint value)
		{
			byte[] bytes = BitConverter.GetBytes(value);

			if (false == BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes);
			}

			return bytes;
		}

		public static byte[] GetBytes(float value)
		{
			byte[] bytes = BitConverter.GetBytes(value);

			if (false == BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes);
			}

			return bytes;
		}

		public static byte[] GetBytes(double value)
		{
			byte[] bytes = BitConverter.GetBytes(value);

			if (false == BitConverter.IsLittleEndian)
			{
				Array.Reverse(bytes);
			}

			return bytes;
		}

		public static float GetSingle(byte[] bytes)
		{
			// Given bytes are little endian,
			// if this computer is big endian then result need to be reversed
			if (false == BitConverter.IsLittleEndian)
			{
				byte[] bytesClone = new byte[bytes.Length];
				bytes.CopyTo(array: bytesClone, index: 0);
				Array.Reverse(bytesClone);
				
				return BitConverter.ToSingle(value: bytesClone, startIndex: 0);
			}
			else
			{
				return BitConverter.ToSingle(value: bytes, startIndex: 0);
			}
		}

		public static double GetDouble(byte[] bytes)
		{
			// Given bytes are little endian,
			// if this computer is big endian then result need to be reversed
			if (false == BitConverter.IsLittleEndian)
			{
				byte[] bytesClone = new byte[bytes.Length];
				bytes.CopyTo(array: bytesClone, index: 0);
				Array.Reverse(bytesClone);
				
				return BitConverter.ToDouble(value: bytesClone, startIndex: 0);
			}
			else
			{
				return BitConverter.ToDouble(value: bytes, startIndex: 0);
			}
		}

		public static long GetInt64(byte[] bytes)
		{
			// Given bytes are little endian,
			// if this computer is big endian then result need to be reversed
			if (false == BitConverter.IsLittleEndian)
			{
				var bytesClone = new byte[bytes.Length];
				bytes.CopyTo(array: bytesClone, index: 0);
				Array.Reverse(bytesClone);
				
				return BitConverter.ToInt64(value: bytesClone, startIndex: 0);
			}
			else
			{
				return BitConverter.ToInt64(value: bytes, startIndex: 0);
			}
		}

		public static int GetInt32(byte[] bytes)
		{
			// Given bytes are little endian,
			// if this computer is big endian then result need to be reversed
			if (false == BitConverter.IsLittleEndian)
			{
				var bytesClone = new byte[bytes.Length];
				bytes.CopyTo(array: bytesClone, index: 0);
				Array.Reverse(bytesClone);
				
				return BitConverter.ToInt32(value: bytesClone, startIndex: 0);
			}
			else
			{
				return BitConverter.ToInt32(value: bytes, startIndex: 0);
			}
		}

		public static uint GetUInt32(byte[] bytes)
		{
			// Given bytes are little endian,
			// if this computer is big endian then result need to be reversed
			if (false == BitConverter.IsLittleEndian)
			{
				var bytesClone = new byte[bytes.Length];
				bytes.CopyTo(array: bytesClone, index: 0);
				Array.Reverse(bytesClone);
				
				return BitConverter.ToUInt32(value: bytesClone, startIndex: 0);
			}
			else
			{
				return BitConverter.ToUInt32(value: bytes, startIndex: 0);
			}
		}

		public static int GetInt32(byte[] bytes, int offset, int count)
		{
			var copied = new byte[count];
			Buffer.BlockCopy(src: bytes, srcOffset: offset, dst: copied, dstOffset: 0, count: count);
			
			return GetInt32(copied);
		}
	}
}