namespace CustomDatabase.Helpers
{
    static class TreeHelper
    {
        public static void RemoveRange<T>(this List<T> target, int fromIndex)
        {
            target.RemoveRange(fromIndex, target.Count - fromIndex);
        }

        public static int BinarySearchFirst<T>(this List<T> array, T value, IComparer<T> comparer)
        {
            if (comparer == null)
            { throw new ArgumentNullException("comparer"); }

            int result = array.BinarySearch(value, comparer);

            if (result >= 1)
            {
                int lastIndex = result;

                for (int i = (result - 1); i >= 0; i--)
                {
                    if (comparer.Compare(array[i], value) != 0)
                    { break; }
                    else
                    { lastIndex = i; }
                }

                result = lastIndex;
            }

            return result;
        }

        public static int BinarySearchLast<T>(this List<T> array, T value, IComparer<T> comparer)
        {
            if (comparer == null)
            { throw new ArgumentNullException("comparer"); }

            int result = array.BinarySearch(value, comparer);

            if (result >= 0 && (result + 1) < array.Count)
            {
                int lastIndex = result;

                for (int i = result + 1; i < array.Count; i++)
                {
                    if (comparer.Compare(array[i], value) != 0)
                    { break; }
                    else
                    { lastIndex = i; }
                }

                result = lastIndex;
            }

            return result;
        }
    }
}