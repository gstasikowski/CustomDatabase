using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CustomDatabase.Logic.Tree
{
    public sealed class TreeDiskNodeManager<K, V> : ITreeNodeManager<K, V>
    {
        #region Variables
        readonly IRecordStorage recordStorage;
        readonly Dictionary<uint, TreeNode<K, V>> dirtyNodes = new Dictionary<uint, TreeNode<K, V>>();
        readonly Dictionary<uint, WeakReference<TreeNode<K, V>>> nodeWeakRefs = new Dictionary<uint, WeakReference<TreeNode<K, V>>>();
        readonly Queue<TreeNode<K, V>> nodeStrongRefs = new Queue<TreeNode<K, V>>();
        readonly int maxStrongNodeRefs = 200;
        readonly TreeDiskNodeSerializer<K, V> serializer;
        readonly ushort minEntriesPerNode = 36;
                
        TreeNode<K, V> rootNode;
        int cleanupCounter = 0;
        #endregion

        #region Getters/Setters
        public ushort MinEntriesPerNode
        {
            get { return minEntriesPerNode; }
        }

        public IComparer<Tuple<K, V>> EntryComparer
        {
            get;
            private set;
        }

        public IComparer<K> KeyComparer
        {
            get;
            private set;
        }

        public TreeNode<K, V> RootNode
        {
            get { return rootNode; }
        }
        #endregion

        #region Constructors
        /// <summary>
        /// Construct a tree from given storage using default comparer for keys.
        /// </summary>
        public TreeDiskNodeManager(ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IRecordStorage nodeStorage)
            : this (keySerializer, valueSerializer, nodeStorage, Comparer<K>.Default)
        { }

        /// <summary>
        /// Construct a tree from given storage using specified comparer for keys.
        /// </summary>
        /// <param name="keySerializer">Tool to serialize node keys.</param>
        /// <param name="valueSerializer">Tool to serialize node values.</param>
        /// <param name="recordStorage">Underlying tool for node storage.</param>
        /// <param name="keyComparer">Key comparer.</param>
        public TreeDiskNodeManager(ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IRecordStorage recordStorage,
            IComparer<K> keyComparer)
        {
            if (recordStorage == null)
            { throw new ArgumentNullException("nodeStorage"); }

            this.serializer = new TreeDiskNodeSerializer<K, V>(this, keySerializer, valueSerializer);
            this.recordStorage = recordStorage;
            this.KeyComparer = keyComparer;
            this.EntryComparer = Comparer<Tuple<K, V>>.Create((a, b) =>
            { return KeyComparer.Compare(a.Item1, b.Item1); });

            // The first record of nodeStorage contains ID of root node.
            // If this record doesn't currently exist, attempt to create it.
            var firstBlockData = recordStorage.Find(1u);

            if (firstBlockData != null)
            { this.rootNode = Find(BufferHelper.ReadBufferUInt32(firstBlockData, 0)); }
            else
            { this.rootNode = CreateFirstRoot(); }
        }
        #endregion

        #region Methods (public)
        public TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIDs)
        {
            TreeNode<K, V> node = null;

            recordStorage.Create(nodeID =>
            {
                node = new TreeNode<K, V>(this, nodeID, 0, entries, childrenIDs);
                OnNodeInitialized(node);

                return this.serializer.Serialize(node);
            });

            if (node == null)
            { throw new Exception("dataGenerator never called by nodeStorage."); }

            return node;
        }

        public TreeNode<K, V> Find(uint ID)
        {
            // Check if the node is being held in memory, return it if true.
            if (nodeWeakRefs.ContainsKey(ID))
            {
                TreeNode<K, V> node;

                if (nodeWeakRefs[ID].TryGetTarget(out node))
                { return node; }
                else
                { 
                    // Node deallocated, remove weak reference.
                    nodeWeakRefs.Remove(ID);
                }
            }

            // If node note in memory, get it.
            var data = recordStorage.Find(ID);

            if (data == null)
            { return null; }

            var deserializedNode = this.serializer.Deserialize(ID, data);

            OnNodeInitialized(deserializedNode);
            
            return deserializedNode;
        }

        public TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeID, uint rightNodeID)
        {
            var node = Create(new Tuple<K, V>[] { new Tuple<K, V>(key, value) }, new uint[] { leftNodeID, rightNodeID });

            this.rootNode = node;
            recordStorage.Update(1u, LittleEndianByteOrder.GetBytes(node.ID));

            return this.rootNode;
        }

        public void MakeRoot(TreeNode<K, V> node)
        {
            this.rootNode = node;
            recordStorage.Update(1u, LittleEndianByteOrder.GetBytes(node.ID));
        }

        public void Delete(TreeNode<K, V> node)
        {
            if (node == rootNode)
            { rootNode = null; }

            recordStorage.Delete(node.ID);

            if (dirtyNodes.ContainsKey(node.ID))
            { dirtyNodes.Remove(node.ID); }
        }

        public void MarkAsChanged(TreeNode<K, V> node)
        {
            if (!dirtyNodes.ContainsKey(node.ID))
            { dirtyNodes.Add(node.ID, node); }
        }

        public void SaveChanges()
        {
            foreach (var kv in dirtyNodes)
            { recordStorage.Update(kv.Value.ID, this.serializer.Serialize(kv.Value)); }

            dirtyNodes.Clear();
        }
        #endregion

        #region Methods (private)
        TreeNode<K, V> CreateFirstRoot()
        {
            // Write down the ID of first node into the first block.
            recordStorage.Create(LittleEndianByteOrder.GetBytes((uint)2));

            // Newely created node should have ID = 2.
            return Create(null, null);
        }

        void OnNodeInitialized(TreeNode<K, V> node)
        {
            // Keep a weak reference to the provided node.
            nodeWeakRefs.Add(node.ID, new WeakReference<TreeNode<K, V>>(node));

            // Keep a stron reference to prevent the weak one from being deallocated.
            nodeStrongRefs.Enqueue(node);

            // Clean up strong refs if there are too many of them.
            if (nodeStrongRefs.Count() >= maxStrongNodeRefs)
            {
                while (nodeStrongRefs.Count() >= (maxStrongNodeRefs/2f))
                { nodeStrongRefs.Dequeue(); }
            }

            // Clean up weak refs
            if (this.cleanupCounter++ >= 1000)
            {
                this.cleanupCounter = 0;
                var toBeDeleted = new List<uint>();

                foreach (var kv in this.nodeWeakRefs)
                {
                    TreeNode<K, V> target;

                    if (!kv.Value.TryGetTarget(out target))
                    { toBeDeleted.Add(kv.Key); }
                }

                foreach (var key in toBeDeleted)
                {
                    this.nodeWeakRefs.Remove(key);
                }
            }
        }
        #endregion
    }
}
