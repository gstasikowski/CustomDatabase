using CustomDatabase.Exceptions;
using CustomDatabase.Logic;

namespace CustomDatabase.UnitTests
{
    public class BlockStorageTests
    {
		private MemoryStream _memoryStream;
		private BlockStorage _storage;
		private List<Block> _testBlocks;

		public BlockStorageTests()
		{
			_memoryStream = new MemoryStream();
			_storage = new BlockStorage(_memoryStream);
			_testBlocks = new List<Block>();
		}

		private void CreateBlockStorage()
		{
			_storage = new BlockStorage(_memoryStream);
		}

		private void PrepareFirstBlockStorage(int count)
		{
			for (int index = 0; index < count; index++)
			{
				var tempBlock = _storage.CreateNew() as Block;
				_testBlocks.Add(tempBlock);
				_testBlocks.Last().SetHeader(field: 1, value: index*10);
			}
		}

		[Theory]
		[InlineData(0)]
		[InlineData(1)]
		[InlineData(2)]
        public void Should_find_created_blocks(int indexToCheck)
		{
			PrepareFirstBlockStorage(3);
			
			Assert.Equal(expected: (uint)indexToCheck, actual: (uint)_testBlocks[indexToCheck].Id);
		}

		[Theory]
		[InlineData(0, 0)]
		[InlineData(1, 10)]
		[InlineData(2, 20)]
		public void Should_confirm_block_headers(uint indexToCheck, int expectedValue)
		{
			PrepareFirstBlockStorage(3);
			
			Assert.Equal(
				expected: (uint)expectedValue,
				actual: (uint)_storage.Find(indexToCheck).GetHeader(1)
			);
		}

		[Fact]
		public void Should_contain_correct_block_size()
		{
			PrepareFirstBlockStorage(3);

			Assert.Equal(expected: _storage.BlockSize * 3, actual: _memoryStream.Length);
		}

		[Fact]
		public void Should_properly_dispose_block()
		{
			PrepareFirstBlockStorage(3);
			var firstBlock = _storage.Find(0);
			firstBlock.Dispose();

			Assert.NotEqual(expected: firstBlock, actual: _storage.Find(0));
		}
    }
}