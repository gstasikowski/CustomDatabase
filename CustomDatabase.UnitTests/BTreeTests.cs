using CustomDatabase.Exceptions;
using CustomDatabase.Logic;

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

		protected virtual void PopulateTree(){}
		public virtual void Should_return_element_number_from_largerThan(int indexToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_largerThanOrEqual(int indexToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_lessThan(int indexToCheck, int expectedResult){}
		public virtual void Should_return_element_number_from_lessThanOrEqual(int indexToCheck, int expectedResult){}
	}

	public class When_testing_empty_tree : BTreeTestBase
	{
		const int IndexToCheck = 3;

        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_largerThan(int indexToCheck, int expectedResult)
        {
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThan(indexToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_largerThanOrEqual(int indexToCheck, int expectedResult)
		{   
			Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThanOrEqualTo(indexToCheck) select node).Count()
			);
		}

        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_lessThan(int indexToCheck, int expectedResult)
		{
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThan(indexToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(IndexToCheck, 0)]
        public override void Should_return_element_number_from_lessThanOrEqual(int indexToCheck, int expectedResult)
		{
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThanOrEqualTo(indexToCheck) select node).Count()
			);
        }
	}

	public class When_testing_tree_insertion : BTreeTestBase
	{
		protected override void PopulateTree()
		{
			testTree.Insert (key: 1, value: "first element");	
		}

		[Fact]
		public void Should_return_tree_element_count()
		{
			PopulateTree();

			Assert.Equal(
				expected: 1,
				actual: testTree.GetAll().Count()
			);
		}
	}

	public class When_testing_non_full_root_tree : BTreeTestBase
	{
		const int MaxIndexToCheck = 8;
		const int MidIndexToCheck = 4;
		const int MinIndexToCheck = 2;

		protected override void PopulateTree()
		{
			testTree.Insert(key: 2, value: "first element");
			testTree.Insert(key: 3, value: "second element");
			testTree.Insert(key: 7, value: "final element");
		}

        [Theory]
		[InlineData(MaxIndexToCheck, 0)]
		[InlineData(MidIndexToCheck, 1)]
		[InlineData(MinIndexToCheck, 2)]
        public override void Should_return_element_number_from_largerThan(int indexToCheck, int expectedResult)
        {
			PopulateTree();
			
            Assert.Equal(
				expected: expectedResult,
				// figure out what exactly 'from .. select node' means (as in, how could it be replaced)
				actual: (from node in testTree.LargerThan(indexToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(MaxIndexToCheck, 0)]
		[InlineData(MidIndexToCheck, 1)]
		[InlineData(MinIndexToCheck, 3)]
        public override void Should_return_element_number_from_largerThanOrEqual(int indexToCheck, int expectedResult)
		{
			PopulateTree();
			
			Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LargerThanOrEqualTo(indexToCheck) select node).Count()
			);
		}

        [Theory]
		[InlineData(MaxIndexToCheck, 3)]
		[InlineData(MidIndexToCheck, 2)]
		[InlineData(MinIndexToCheck, 0)]
        public override void Should_return_element_number_from_lessThan(int indexToCheck, int expectedResult)
		{
			PopulateTree();
			
            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThan(indexToCheck) select node).Count()
			);
		}
		
        [Theory]
		[InlineData(MaxIndexToCheck, 3)]
		[InlineData(MidIndexToCheck, 2)]
		[InlineData(MinIndexToCheck, 1)]
        public override void Should_return_element_number_from_lessThanOrEqual(int indexToCheck, int expectedResult)
		{
			PopulateTree();

            Assert.Equal(
				expected: expectedResult,
				actual: (from node in testTree.LessThanOrEqualTo(indexToCheck) select node).Count()
			);
        }
		
        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{})]
		[InlineData(MidIndexToCheck, new int[]{ 7 })]
		[InlineData(MinIndexToCheck, new int[]{ 3, 7 })]
		public void Should_return_element_collection_from_largerThan(int indexToCheck, int[] expectedResult)
		{
			PopulateTree();
			
			Assert.True((from node in testTree.LargerThan(indexToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{})]
		[InlineData(MidIndexToCheck, new int[]{ 7 })]
		[InlineData(MinIndexToCheck, new int[]{ 2, 3, 7 })]
		public void Should_return_element_collection_from_largerThanOrEqualTo(int indexToCheck, int[] expectedResult)
		{
			PopulateTree();
			
			Assert.True((from node in testTree.LargerThanOrEqualTo(indexToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{ 7, 3, 2})]
		[InlineData(MidIndexToCheck, new int[]{ 3, 2 })]
		[InlineData(MinIndexToCheck, new int[]{})]
		public void Should_return_element_collection_from_lessThan(int indexToCheck, int[] expectedResult)
		{
			PopulateTree();
			
			Assert.True((from node in testTree.LessThan(indexToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

        [Theory]
		[InlineData(MaxIndexToCheck, new int[]{ 7, 3, 2 })]
		[InlineData(MidIndexToCheck, new int[]{ 3, 2 })]
		[InlineData(MinIndexToCheck, new int[]{ 2 })]
		public void Should_return_element_collection_from_lessThanOrEqualTo(int indexToCheck, int[] expectedResult)
		{
			PopulateTree();
			
			Assert.True((from node in testTree.LessThanOrEqualTo(indexToCheck) select node.Item1).SequenceEqual(expectedResult));
		}

		public void Should_throw_key_exists_exception()
		{
			Assert.Throws<TreeKeyExistsException>(delegate {
				testTree.Insert(key: 7, value: "duplicate element");
			});
		}
    }
}