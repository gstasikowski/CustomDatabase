using CustomDatabase.Logic.Tree;
using System;
using System.Collections.Generic;

namespace CustomDatabase.Interfaces
{
    public interface ITreeNodeManager<K, V>
    {
        /// <summary>
        /// Minimum number of entries per node.
        /// Maximum must be equal MinEntriesPerNode * 2
        /// </summary>
        ushort MinEntriesPerNode
        { get; }

        /// <summary>
        /// Get the comparer used to compare the keys.
        /// </summary>
        IComparer<K> KeyComparer
        { get; }

        /// <summary>
        /// Get the comparer used to compare entries.
        /// Must use KeyComparer declared above.
        /// </summary>
        IComparer<Tuple<K, V>> EntryComparer
        { get; }

        /// <summary>
        /// Get the root node - must be cached as it always gets called.
        /// </summary>
        TreeNode<K, V> RootNode
        { get; }

        /// <summary>
        /// Creates a new node that carries given entries 
        /// and keeps references to given children nodes.
        /// </summary>
        /// <param name="entries">Entries</param>
        /// <param name="childrenIDs">Children identifiers.</param>
        TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIDs);

        /// <summary>
        /// Find a node by its ID.
        /// </summary>
        TreeNode<K, V> Find(uint ID);

        /// <summary>
        /// Called by the tree to split current root node to a new one.
        /// </summary>
        /// <param name="leftNodeID">Left node identifier.</param>
        /// <param name="rightNodeID">Right node identifier.</param>
        TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeID, uint rightNodeID);

        /// <summary>
        /// Make given node into root.
        /// </summary>
        void MakeRoot(TreeNode<K, V> node);

        /// <summary>
        /// Mark given node as modified, for saving.
        /// </summary>
        /// <param name="node">Node</param>
        void MarkAsChanged(TreeNode<K, V> node);

        /// <summary>
        /// Delete specified node.
        /// </summary>
        void Delete(TreeNode<K, V> node);

        /// <summary>
        /// Write all modified nodes to disk.
        /// </summary>
        void SaveChanges();
    }
}
