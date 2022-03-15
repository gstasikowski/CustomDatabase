using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;

namespace CustomDatabase.Logic.Tree
{
    public class TreeMemoryNodeManager<K, V> : ITreeNodeManager<K, V>
    {
        #region Variables
        readonly Dictionary<uint, TreeNode<K, V>> nodes = new Dictionary<uint, TreeNode<K, V>>();
        readonly ushort minEntriesCountPerNode;
        readonly IComparer<K> keyComparer;
        readonly IComparer<Tuple<K, V>> entryComparer;

        int idCounter = 1;
        TreeNode<K, V> rootNode;
        #endregion Variables

        #region Properties
        public ushort MinEntriesPerNode
        {
            get { return minEntriesCountPerNode; }
        }

        public IComparer<K> KeyComparer
        {
            get { return keyComparer; }
        }

        public IComparer<Tuple<K, V>> EntryComparer
        {
            get { return entryComparer; }
        }

        public TreeNode<K, V> RootNode
        {
            get { return rootNode; }
        }
        #endregion Properties

        #region Constructor
        /// <param name="minEntriesCountPerNode">Multiplied by 2 is the degree of the tree.</param>
        /// <param name="keyComparer">Key comparer.</param>
        public TreeMemoryNodeManager(ushort minEntriesCountPerNode, IComparer<K> keyComparer)
        {
            this.minEntriesCountPerNode = minEntriesCountPerNode;
            this.keyComparer = keyComparer;
            this.entryComparer = Comparer<Tuple<K, V>>.Create((t1, t2) =>
            { return this.keyComparer.Compare(t1.Item1, t2.Item1); });

            this.rootNode = Create(null, null);
        }
        #endregion Constructor

        #region Methods (public)
        public TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIDs)
        {
            var newNode = new TreeNode<K, V>(this, (uint)(this.idCounter++), 0, entries, childrenIDs);

            nodes[newNode.ID] = newNode;

            return newNode;
        }

        public TreeNode<K, V> Find(uint ID)
        {
            if (!nodes.ContainsKey(ID))
            { throw new ArgumentException("Node not found by ID: " + ID); }

            return nodes[ID];
        }

        public TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeID, uint rightNodeID)
        {
            var newNode = Create(new Tuple<K, V>[] { new Tuple<K, V>(key, value) }, new uint[] { leftNodeID, rightNodeID });

            this.rootNode = newNode;

            return newNode;
        }

        public void Delete(TreeNode<K, V> target)
        {
            if (target == rootNode)
            { rootNode = null; }

            if (nodes.ContainsKey(target.ID))
            { nodes.Remove(target.ID); }
        }

        public void MakeRoot(TreeNode<K, V> target)
        {
            this.rootNode = target;
        }

        public void MarkAsChanged(TreeNode<K, V> target)
        {
            // dummy method
        }

        public void SaveChanges()
        {
            // dummy method
        }
        #endregion Methods (public)
    }
}
