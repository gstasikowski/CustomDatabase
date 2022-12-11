namespace CustomDatabase.Helpers
{
    static class TreeHelper
    {
        public static void RemoveRange<T>(this List<T> target, int fromIndex)
        {
            target.RemoveRange(index: fromIndex, count: target.Count - fromIndex);
        }

        public static int BinarySearchFirst<T>(this List<T> array, T value, IComparer<T> comparer)
        {
            if (comparer == null)
            {
                throw new ArgumentNullException("comparer");
            }

            int result = array.BinarySearch(item: value, comparer: comparer);

            if (result >= 1)
            {
                int lastIndex = result;

                for (int index = (result - 1); index >= 0; index--)
                {
                    if (comparer.Compare(x: array[index], y: value) != 0)
                    {
                        break;
                    }
                    else
                    {
                        lastIndex = index;
                    }
                }

                result = lastIndex;
            }

            return result;
        }

        public static int BinarySearchLast<T>(this List<T> array, T value, IComparer<T> comparer)
        {
            if (comparer == null)
            {
                throw new ArgumentNullException("comparer");
            }

            int result = array.BinarySearch(item: value, comparer: comparer);

            if (result >= 0 && (result + 1) < array.Count)
            {
                int lastIndex = result;

                for (int index = result + 1; index < array.Count; index++)
                {
                    if (comparer.Compare(x: array[index], y: value) != 0)
                    {
                        break;
                    }
                    else
                    {
                        lastIndex = index;
                    }
                }

                result = lastIndex;
            }

            return result;
        }
    }
}