using CustomDatabase.Exceptions;
using CustomDatabase.Logic;

// Still trying to figure out how to structure this.
namespace CustomDatabase.UnitTests
{
    public class BTreeTestBase
    {
		protected Tree<int, string> testTree = new Tree<int, string>(
				nodeManager: new TreeMemoryNodeManager<int, string>(
					minEntriesCountPerNode: 2,
					keyComparer: Comparer<int>.Default
				),
				allowDuplicateKeys: false
			);

		protected void PopulateTree(int count)
		{
			for (int index = 0; index < count; index++)
			{
				testTree.Insert(index, "element" + index);	
			}
		}
		
		public virtual void Should_return_element_number_from_largerThanOrEqual(int keyToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_largerThan(int keyToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_lessThanOrEqual(int keyToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_lessThan(int keyToCheck, int expectedResult){}
	}

	public class When_testing_empty_unique_tree : BTreeTestBase
	{
		const int IndexToCheck = 3;
		
        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_largerThanOrEqual(int keyToCheck, int expectedResult)
		{   
			Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThanOrEqualTo(keyToCheck) select node).Count()
			);
		}

        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_largerThan(int keyToCheck, int expectedResult)
        {
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThan(keyToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_lessThanOrEqual(int keyToCheck, int expectedResult)
		{
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThanOrEqualTo(keyToCheck) select node).Count()
			);
        }

        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_lessThan(int keyToCheck, int expectedResult)
		{
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThan(keyToCheck) select node).Count()
			);
		}
	}

	public class When_testing_populated_unique_tree : BTreeTestBase
	{
		const int MaxIndexToCheck = 8;
		const int MidIndexToCheck = 4;
		const int MinIndexToCheck = 2;

        [Theory]
		[InlineData(MaxIndexToCheck, 0)]
		[InlineData(MidIndexToCheck, 2)]
		[InlineData(MinIndexToCheck, 4)]
        public override void Should_return_element_number_from_largerThanOrEqual(int keyToCheck, int expectedResult)
		{
			PopulateTree(6);
			
			Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThanOrEqualTo(keyToCheck) select node).Count()
			);
		}

        [Theory]
		[InlineData(MaxIndexToCheck, 0)]
		[InlineData(MidIndexToCheck, 2)]
		[InlineData(MinIndexToCheck, 4)]
        public override void Should_return_element_number_from_largerThan(int keyToCheck, int expectedResult)
        {
			PopulateTree(7);
			
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThan(keyToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(MaxIndexToCheck, 4)]
		[InlineData(MidIndexToCheck, 4)]
		[InlineData(MinIndexToCheck, 3)]
        public override void Should_return_element_number_from_lessThanOrEqual(int keyToCheck, int expectedResult)
		{
			PopulateTree(4);

            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThanOrEqualTo(keyToCheck) select node).Count()
			);
        }

        [Theory]
		[InlineData(MaxIndexToCheck, 4)]
		[InlineData(MidIndexToCheck, 4)]
		[InlineData(MinIndexToCheck, 2)]
        public override void Should_return_element_number_from_lessThan(int keyToCheck, int expectedResult)
		{
			PopulateTree(4);
			
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThan(keyToCheck) select node).Count()
			);
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{})]
		[InlineData(MidIndexToCheck, new int[]{ 4, 5, 6, 7 })]
		[InlineData(MinIndexToCheck, new int[]{ 2, 3, 4, 5, 6, 7 })]
		public void Should_return_element_collection_from_largerThanOrEqualTo(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(8);
			
			Assert.True((from node in testTree.LargerThanOrEqualTo(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}
		
        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{})]
		[InlineData(MidIndexToCheck, new int[]{ 5, 6, 7 })]
		[InlineData(MinIndexToCheck, new int[]{ 3, 4, 5, 6, 7 })]
		public void Should_return_element_collection_from_largerThan(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(8);
			
			Assert.True((from node in testTree.LargerThan(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{ 3, 2, 1, 0 })]
		[InlineData(MidIndexToCheck, new int[]{ 3, 2, 1, 0 })]
		[InlineData(MinIndexToCheck, new int[]{ 2, 1, 0 })]
		public void Should_return_element_collection_from_lessThanOrEqualTo(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(4);
			
			Assert.True((from node in testTree.LessThanOrEqualTo(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{ 3, 2, 1, 0 })]
		[InlineData(MidIndexToCheck, new int[]{ 3, 2, 1, 0 })]
		[InlineData(MinIndexToCheck, new int[]{ 1, 0 })]
		public void Should_return_element_collection_from_lessThan(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(4);
			
			Assert.True((from node in testTree.LessThan(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

		[Fact]
		public void Should_throw_key_exists_exception()
		{
			PopulateTree(4);

			Assert.Throws<TreeKeyExistsException>(delegate {
				testTree.Insert(key: 2, value: "duplicate element");
			});
		}
    }

	public class When_testing_root_node_splitting : BTreeTestBase
	{
		const int CorrectIndex1 = 0;
		const int CorrectIndex2 = 1;
		const int CorrectIndex3 = 2;
		const int CorrectIndex4 = 3;
		const int CorrectIndex5 = 4;

		const int IncorrectIndex1 = 10;
		const int IncorrectIndex2 = 8;
		const int IncorrectIndex3 = 60;
		const int IncorrectIndex4 = -3;
		
		[Theory]
		[InlineData(CorrectIndex1)]
		[InlineData(CorrectIndex2)]
		[InlineData(CorrectIndex3)]
		[InlineData(CorrectIndex4)]
		[InlineData(CorrectIndex5)]
		public void Should_find_entries_by_key(int keyToCheck)
		{
			PopulateTree(10);

			Assert.NotNull(testTree.Get (keyToCheck));
		}

		[Theory]
		[InlineData(IncorrectIndex1)]
		[InlineData(IncorrectIndex2)]
		[InlineData(IncorrectIndex3)]
		[InlineData(IncorrectIndex4)]
		public void Should_not_find_entries_by_key(int keyToCheck)
		{
			PopulateTree(6);

			Assert.Null(testTree.Get(keyToCheck));
		}

        [Theory]
		[InlineData(CorrectIndex1, new int[]{ 1, 2, 3, 4 })]
		[InlineData(CorrectIndex2, new int[]{ 2, 3, 4 })]
		[InlineData(CorrectIndex3, new int[]{ 3, 4 })]
		[InlineData(CorrectIndex4, new int[]{ 4 })]
		[InlineData(CorrectIndex5, new int[]{})]
		public void Should_return_element_collection_from_largerThan(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(5);
			
			Assert.True((from node in testTree.LargerThan(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(CorrectIndex1, new int[]{ 0, 1, 2, 3, 4 })]
		[InlineData(CorrectIndex2, new int[]{ 1, 2, 3, 4 })]
		[InlineData(CorrectIndex3, new int[]{ 2, 3, 4 })]
		[InlineData(CorrectIndex4, new int[]{ 3, 4 })]
		[InlineData(CorrectIndex5, new int[]{ 4 })]
		public void Should_return_element_collection_from_largerThanOrEqualTo(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(5);
			
			Assert.True((from node in testTree.LargerThanOrEqualTo(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(CorrectIndex1, new int[]{})]
		[InlineData(CorrectIndex2, new int[]{ 0 })]
		[InlineData(CorrectIndex3, new int[]{ 1, 0 })]
		public void Should_return_element_collection_from_lessThan(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(5);

			Assert.True((from node in testTree.LessThan(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(CorrectIndex1, new int[]{ 0 })]
		[InlineData(CorrectIndex2, new int[]{ 1, 0 })]
		public void Should_return_element_collection_from_lessThanOrEqualTo(int keyToCheck, int[] expectedResult)
		{
			PopulateTree(5);
			
			Assert.True((from node in testTree.LessThanOrEqualTo(keyToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

		[Fact]
		public void Should_split_child_node()
		{
			for (int index = 0; index <= 100; index++)
			{
				testTree.Insert (index, index.ToString());
				var result = (from tuple in testTree.LargerThanOrEqualTo(0) select tuple.Item1).ToList();
				Assert.Equal (index + 1, result.Count);
			}
		}
	}
}