using CustomDatabase.Exceptions;
using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;

namespace CustomDatabase.Logic.Tree
{
    public class Tree<K, V> : IIndex<K, V>
    {
        #region Variables
        readonly ITreeNodeManager<K, V> nodeManager;
        readonly bool allowDuplicateKeys;
        #endregion Variables

        #region Constructor
        public Tree(ITreeNodeManager<K, V> nodeManager, bool allowDuplicateKeys)
        {
            if (nodeManager == null)
            { throw new ArgumentNullException("nodeManager"); }

            this.nodeManager = nodeManager;
            this.allowDuplicateKeys = allowDuplicateKeys;
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Delete specified entry.
        /// </summary>
        public bool Delete(K key, V value, IComparer<V> valueComparer = null)
        {
            if (!allowDuplicateKeys)
            { throw new InvalidOperationException("This method should be called only from non unique tree."); }

            valueComparer = (valueComparer == null) ? Comparer<V>.Default : valueComparer;

            var deleted = false;
            var shouldContinue = true;

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
                            if (nodeManager.KeyComparer.Compare(entry.Item1, key) > 0)
                            {
                                shouldContinue = false;
                                break;
                            }

                            // Delete entry if matches what we look for.
                            if (valueComparer.Compare(entry.Item2, value) == 0)
                            {
                                enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
                                deleted = true;
                                break;
                            }
                        }
                    }
                }
            }
            catch (EndEnumeratingException)
            { }

            nodeManager.SaveChanges();
            return deleted;
        }

        /// <summary>
        /// Delete all entries of given key.
        /// </summary>
        public bool Delete(K key)
        {
            if (allowDuplicateKeys)
            { throw new InvalidOperationException("This method should be called only from unique tree."); }

            using (var enumerator = (TreeEnumerator<K, V>)LargerThanOrEqualTo(key).GetEnumerator())
            {
                if (enumerator.MoveNext() && nodeManager.KeyComparer.Compare(enumerator.Current.Item1, key) == 0)
                {
                    enumerator.CurrentNode.Remove(enumerator.CurrentEntry);
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
            var insertionIndex = 0;
            var leafNode = FindNodeForInsertion(key, ref insertionIndex);

            if (insertionIndex >= 0 && !allowDuplicateKeys)
            { throw new TreeKeyExistsException(key); }

            leafNode.InsertAsLeaf(key, value, (insertionIndex >= 0) ? insertionIndex : ~insertionIndex);

            // Split the leaf in case of overflow
            if (leafNode.IsOverflow)
            {
                TreeNode<K, V> left, right;
                leafNode.Split(out left, out right);
            }

            nodeManager.SaveChanges();
        }

        /// <summary>
        /// Find entry by its key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns>Node entry or null.</returns>
        public Tuple<K, V> Get(K key)
        {
            var insertionIndex = 0;
            var node = FindNodeForInsertion(key, ref insertionIndex);

            if (insertionIndex < 0)
            { return null; }

            return node.GetEntry(insertionIndex);
        }

        /// <summary>
        /// Get all entries in the database.
        /// </summary>
        public IEnumerable<Tuple<K, V>> GetAll()
        {
            return nodeManager.RootNode.Entries;
        }

        /// <summary>
        /// Search for all elements >= to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LargerThanOrEqualTo(K key)
        {
            var startIterationIndex = 0;
            var node = FindNodeForIteration(key, this.nodeManager.RootNode, true, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager,
                node,
                ((startIterationIndex >= 0) ? startIterationIndex : ~startIterationIndex) - 1,
                TreeTraverseDirection.Ascending);
        }

        /// <summary>
        /// Search for all elements > to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LargerThan(K key)
        {
            var startIterationIndex = 0;
            var node = FindNodeForIteration(key, this.nodeManager.RootNode, false, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager,
                node,
                (startIterationIndex >= 0) ? startIterationIndex : (~startIterationIndex - 1),
                TreeTraverseDirection.Ascending);
        }

        /// <summary>
        /// Search for all elements <= to given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LessThanOrEqualTo(K key)
        {
            var startIterationIndex = 0;
            var node = FindNodeForIteration(key, this.nodeManager.RootNode, false, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager,
                node,
                (startIterationIndex >= 0) ? (startIterationIndex + 1) : ~startIterationIndex,
                TreeTraverseDirection.Descending);
        }

        /// <summary>
        /// Search for all elements less than given key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IEnumerable<Tuple<K, V>> LessThan(K key)
        {
            var startIterationIndex = 0;
            var node = FindNodeForIteration(key, this.nodeManager.RootNode, true, ref startIterationIndex);

            return new TreeTraverser<K, V>(nodeManager,
                node,
                (startIterationIndex >= 0) ? startIterationIndex : ~startIterationIndex,
                TreeTraverseDirection.Descending);
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
        TreeNode<K, V> FindNodeForIteration(K key, TreeNode<K, V> node, bool moveLeft, ref int startIterationIndex)
        {
            // Return if node is empty (non-full root node).
            // Return value is bitwise complement of 0, not 0 itself to prevent
            // caller from thinking we found a key index.
            if (node.IsEmpty)
            {
                startIterationIndex = ~0;
                return node;
            }

            var binarySearchResult = node.BinarySearchEntriesForKey(key, moveLeft ? true : false);

            if (binarySearchResult >= 0)
            {
                if (node.IsLeaf)
                {
                    startIterationIndex = binarySearchResult;
                    return node;
                }
                else
                {
                    return FindNodeForIteration(key,
                        node.GetChildNode(moveLeft ? binarySearchResult : binarySearchResult + 1),
                        moveLeft,
                        ref startIterationIndex);
                }
            }
            else if (!node.IsLeaf)
            {
                return FindNodeForIteration(key, node.GetChildNode(~binarySearchResult), moveLeft, ref startIterationIndex);
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
        TreeNode<K, V> FindNodeForInsertion(K key, TreeNode<K, V> node, ref int insertionIndex)
        {
            // Return if node is empty (non-full root node).
            // Return value is bitwise complement of 0, not 0 itself to prevent
            // caller from thinking we found a key index.
            if (node.IsEmpty)
            {
                insertionIndex = ~0;
                return node;
            }

            var binarySearchResult = node.BinarySearchEntriesForKey(key);

            if (binarySearchResult >= 0)
            {
                if (allowDuplicateKeys && !node.IsLeaf)
                { return FindNodeForInsertion(key, node.GetChildNode(binarySearchResult), ref insertionIndex); }
                else
                {
                    insertionIndex = binarySearchResult;
                    return node;
                }
            }
            else if (!node.IsLeaf)
            { return FindNodeForInsertion(key, node.GetChildNode(~binarySearchResult), ref insertionIndex); }
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
            return FindNodeForInsertion(key, nodeManager.RootNode, ref insertionIndex);
        }
        #endregion Methods (private)
    }
}
