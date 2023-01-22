using CustomDatabase.Exceptions;
using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class Tree<K, V> : IIndex<K, V>
    {
        #region Variables
        private const int BitwiseComplement = ~0;
        private readonly ITreeNodeManager<K, V> _nodeManager;
        private readonly bool _allowDuplicateKeys;
        #endregion Variables

        #region Constructor
        public Tree(ITreeNodeManager<K, V> nodeManager, bool allowDuplicateKeys)
        {
            if (nodeManager == null)
            {
                throw new ArgumentNullException("nodeManager");
            }

            _nodeManager = nodeManager;
            _allowDuplicateKeys = allowDuplicateKeys;
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Delete specified entry.
        /// </summary>
        public bool Delete(K key, V value, IComparer<V> valueComparer = null)
        {
            if (!_allowDuplicateKeys)
            {
                throw new InvalidOperationException(
                    CommonResources.GetErrorMessage("NonUniqueTreeInvocation")
                );
            }

            valueComparer = (valueComparer == null) ? Comparer<V>.Default : valueComparer;

            bool isDeleted = false;
            bool shouldContinue = true;

            try
            {
                while (shouldContinue)
                {
                    // Looking for all entries we want to remove.
                    using (var enumerator = (TreeEnumerator<K, V>)LargerThanOrEqualTo(key).GetEnumerator())
                    {
                        while (true)
                        {
                            // Stop enumerating upon reaching the end of enumerator.
                            if (!enumerator.MoveNext())
                            {
                                shouldContinue = false;
                                break;
                            }

                            var entry = enumerator.Current;

                            // Stop enumerating upon reaching key larger than we look for.
                            if (_nodeManager.KeyComparer.Compare(x: entry.Item1, y: key) > 0)
                            {
                                shouldContinue = false;
                                break;
                            }

                            // Delete entry if matches what we look for.
                            if (valueComparer.Compare(x: entry.Item2, y: value) == 0)
                            {
                                enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
                                isDeleted = true;
                                break;
                            }
                        }
                    }
                }
            }
            catch (EndEnumeratingException)
            { }

            _nodeManager.SaveChanges();
            return isDeleted;
        }

        /// <summary>
        /// Delete all entries of given key.
        /// </summary>
        public bool Delete(K key)
        {
            if (_allowDuplicateKeys)
            {
                throw new InvalidOperationException(
                    CommonResources.GetErrorMessage("UniqueTreeInvocation")
                );
            }

            using (var enumerator = (TreeEnumerator<K, V>)LargerThanOrEqualTo(key).GetEnumerator())
            {
                if (enumerator.MoveNext() &&
                    _nodeManager.KeyComparer.Compare(x: enumerator.Current.Item1, y: key) == 0
                    )
                {
                    enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
                    _nodeManager.SaveChanges();
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Insert an entry into the tree.
        /// </summary>
        public void Insert(K key, V value)
        {
            int insertionIndex = 0;
            var leafNode = FindNodeForInsertion(key: key, insertionIndex: ref insertionIndex);

            if (insertionIndex >= 0 && !_allowDuplicateKeys)
            {
                throw new TreeKeyExistsException(key);
            }

            leafNode.InsertAsLeaf(
                key: key,
                value: value,
                insertPosition: (insertionIndex >= 0) ? insertionIndex : ~insertionIndex
            );

            // Split the leaf in case of overflow
            if (leafNode.IsOverflow)
            {
                TreeNode<K, V> left, right;
                leafNode.Split(outLeftNode: out left, outRightNode: out right);
            }

            _nodeManager.SaveChanges();
        }

        /// <summary>
        /// Find entry by its key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns>Node entry or null.</returns>
        public Tuple<K, V> Get(K key)
        {
            int insertionIndex = 0;
            var node = FindNodeForInsertion(key: key, insertionIndex: ref insertionIndex);

            if (insertionIndex < 0)
            {
                return null;
            }

            return node.GetEntry(insertionIndex);
        }

        /// <summary>
        /// Get all entries in the database.
        /// </summary>
        public IEnumerable<Tuple<K, V>> GetAll()
        {
            return _nodeManager.RootNode.Entries;
        }

        /// <summary>
        /// Search for all elements >= to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key)
        {
            int startIterationIndex = 0;
            var node = FindNodeForIteration(
                key: key,
                node: _nodeManager.RootNode,
                moveLeft: true,
                startIterationIndex: ref startIterationIndex
            );

            return new TreeTraverser<K, V>(
                nodeManager: _nodeManager,
                fromNode: node,
                fromIndex: ((startIterationIndex >= 0) ? startIterationIndex : ~startIterationIndex) - 1,
                direction: TreeTraverseDirection.Ascending
            );
        }

        /// <summary>
        /// Search for all elements > to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LargerThan(K key)
        {
            int startIterationIndex = 0;
            var node = FindNodeForIteration(
                key: key,
                node: _nodeManager.RootNode,
                moveLeft: false,
                startIterationIndex: ref startIterationIndex
            );

            return new TreeTraverser<K, V>(
                nodeManager: _nodeManager,
                fromNode: node,
                fromIndex: (startIterationIndex >= 0) ? startIterationIndex : (~startIterationIndex - 1),
                direction: TreeTraverseDirection.Ascending
            );
        }

        /// <summary>
        /// Search for all elements <= to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LessThanOrEqualTo(K key)
        {
            int startIterationIndex = 0;
            var node = FindNodeForIteration(
                key: key,
                node: _nodeManager.RootNode,
                moveLeft: false,
                startIterationIndex: ref startIterationIndex
            );

            return new TreeTraverser<K, V>(
                nodeManager: _nodeManager,
                fromNode: node,
                fromIndex: (startIterationIndex >= 0) ? (startIterationIndex + 1) : ~startIterationIndex,
                direction: TreeTraverseDirection.Descending
            );
        }

        /// <summary>
        /// Search for all elements less than given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LessThan(K key)
        {
            int startIterationIndex = 0;
            var node = FindNodeForIteration(
                key: key,
                node: _nodeManager.RootNode,
                moveLeft: true,
                startIterationIndex: ref startIterationIndex
            );

            return new TreeTraverser<K, V>(
                nodeManager: _nodeManager,
                fromNode: node,
                fromIndex: (startIterationIndex >= 0) ? startIterationIndex : ~startIterationIndex,
                direction: TreeTraverseDirection.Descending
            );
        }
        #endregion Methods (public)

        #region Methods (private)
        /// <summary>
        /// Similar to FindNodeForInsertion() but used in case of tree with duplicate keys.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="node"></param>
        /// <param name="moveLeft"></param>
        /// <param name="startIterationIndex"></param>
        /// <returns></returns>
        private TreeNode<K, V> FindNodeForIteration(
            K key,
            TreeNode<K, V> node,
            bool moveLeft,
            ref int startIterationIndex
        )
        {
            // Return if node is empty (non-full root node).
            // Return value is bitwise complement of 0, not 0 itself to prevent
            // caller from thinking we found a key index.
            if (node.IsEmpty)
            {
                startIterationIndex = BitwiseComplement;
                return node;
            }

            int binarySearchResult = node.BinarySearchEntriesForKey(
                key: key,
                firstOccurence: moveLeft ? true : false
            );

            if (binarySearchResult >= 0)
            {
                if (node.IsLeaf)
                {
                    startIterationIndex = binarySearchResult;
                    return node;
                }
                else
                {
                    return FindNodeForIteration(
                        key: key,
                        node: node.GetChildNode(moveLeft ? binarySearchResult : binarySearchResult + 1),
                        moveLeft: moveLeft,
                        startIterationIndex: ref startIterationIndex
                    );
                }
            }
            else if (!node.IsLeaf)
            {
                return FindNodeForIteration(
                    key: key,
                    node: node.GetChildNode(~binarySearchResult),
                    moveLeft: moveLeft,
                    startIterationIndex: ref startIterationIndex
                );
            }
            else
            {
                startIterationIndex = binarySearchResult;
                return node;
            }
        }

        /// <summary>
        /// Search for a node containing the given key, start from given node.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="node"></param>
        /// <param name="insertionIndex"></param>
        private TreeNode<K, V> FindNodeForInsertion(K key, TreeNode<K, V> node, ref int insertionIndex)
        {
            // Return if node is empty (non-full root node).
            // Return value is bitwise complement of 0, not 0 itself to prevent
            // caller from thinking we found a key index.
            if (node.IsEmpty)
            {
                insertionIndex = BitwiseComplement;
                return node;
            }

            int binarySearchResult = node.BinarySearchEntriesForKey(key);

            if (binarySearchResult >= 0)
            {
                if (_allowDuplicateKeys && !node.IsLeaf)
                {
                    return FindNodeForInsertion(
                        key: key,
                        node: node.GetChildNode(binarySearchResult),
                        insertionIndex: ref insertionIndex
                    );
                }
                else
                {
                    insertionIndex = binarySearchResult;
                    return node;
                }
            }
            else if (!node.IsLeaf)
            {
                return FindNodeForInsertion(
                    key: key,
                    node: node.GetChildNode(~binarySearchResult),
                    insertionIndex: ref insertionIndex
                );
            }
            else
            {
                insertionIndex = binarySearchResult;
                return node;
            }
        }

        /// <summary>
        /// Search for a node containing the given key, start from root node.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="insertionIndex"></param>
        TreeNode<K, V> FindNodeForInsertion(K key, ref int insertionIndex)
        {
            return FindNodeForInsertion(
                key: key,
                node: _nodeManager.RootNode,
                insertionIndex: ref insertionIndex
            );
        }
        #endregion Methods (private)
    }
}