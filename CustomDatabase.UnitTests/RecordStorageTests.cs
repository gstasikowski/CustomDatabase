using CustomDatabase.Logic;

namespace CustomDatabase.UnitTests
{
    public class RecordStorageTests
    {
		const int BlockSize = 8192;
		const int BlockHeaderSize = 48;

		private RecordStorage _testRecordStorage;

        private void CreatePopulatedRecordStorage(int blockAmount, int entryNumber = 1)
		{
			_testRecordStorage = new RecordStorage (new BlockStorage(
																	storage: new MemoryStream(),
																	blockSize: BlockSize,
																	blockHeaderSize: BlockHeaderSize
																)
													);

			PopulateRecordStorage(blockAmount, entryNumber);
		}

		private void PopulateRecordStorage(int blockAmount, int entryNumber)
		{
			byte[] blockData;
			
			for (int index = 0; index < entryNumber; index++)
			{
				blockData = GenerateRandomData(CalculateDataToFill(blockAmount));
				_testRecordStorage.Create(blockData);
			}
		}

		private void UpdateRecordStorage(int newBlockAmount)
		{
			byte[] blockData = GenerateRandomData(CalculateDataToFill(newBlockAmount));
			_testRecordStorage.Update(1, blockData);
		}

		private static byte[] GenerateRandomData(int length)
		{
			byte[] data = new byte[length];
			Random random = new Random();
			
			for (int index = 0; index < data.Length; index++)
			{
				data[index] = (byte)random.Next(0, 256);
			}

			return data;
		}

		private static int CalculateDataToFill(int blockAmount)
		{
			return (blockAmount - 1) * BlockSize + (BlockSize / 2);
		}

		[Theory]
		[InlineData(1)]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(10)]
		public void Should_create_populated_record_storage(int blockAmount)
		{
			CreatePopulatedRecordStorage(blockAmount);

			Assert.True(_testRecordStorage.Find(1).Length == CalculateDataToFill(blockAmount));
		}

		[Theory]
		[InlineData(1, 2)]
		[InlineData(2, 1)]
		[InlineData(3, 3)]
		[InlineData(1, 10)]
		[InlineData(10, 1)]
		public void Should_update_existing_record_storage(int blockAmount, int newBlockAmount)
		{
			CreatePopulatedRecordStorage(blockAmount);
			UpdateRecordStorage(newBlockAmount);
		
			Assert.True(_testRecordStorage.Find(1).Length == CalculateDataToFill(newBlockAmount));
		}

		[Fact]
		public void Should_delete_entry_from_record_storage()
		{
			CreatePopulatedRecordStorage(blockAmount: 1, entryNumber: 2);
			_testRecordStorage.Delete(1);

			Assert.True(_testRecordStorage.Find(1) == null);
		}
    }
}