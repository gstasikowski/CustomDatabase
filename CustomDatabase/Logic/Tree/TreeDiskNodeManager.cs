using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic.Tree
{
    public sealed class TreeDiskNodeManager<K, V> : ITreeNodeManager<K, V>
    {
        #region Variables
        private readonly IRecordStorage _recordStorage;
        private readonly Dictionary<uint, TreeNode<K, V>> _dirtyNodes = 
            new Dictionary<uint, TreeNode<K, V>>();
        private readonly Dictionary<uint, WeakReference<TreeNode<K, V>>> _nodeWeakRefs = 
            new Dictionary<uint, WeakReference<TreeNode<K, V>>>();
        private readonly System.Collections.Queue _nodeStrongRefs = new System.Collections.Queue();
        private readonly int _maxStrongNodeRefs = 200;
        private readonly TreeDiskNodeSerializer<K, V> _serializer;
        private readonly ushort _minEntriesPerNode = 36;
                
        private TreeNode<K, V> _rootNode;
        private int _cleanupCounter = 0;
        #endregion Variables

        #region Properties
        public ushort MinEntriesPerNode
        {
            get { return _minEntriesPerNode; }
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
            get { return _rootNode; }
        }
        #endregion Properties

        #region Constructors
        /// <summary>
        /// Construct a tree from given storage using default comparer for keys.
        /// </summary>
        public TreeDiskNodeManager(
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IRecordStorage nodeStorage)
            : this (keySerializer, valueSerializer, nodeStorage, Comparer<K>.Default
        )
        { }

        /// <summary>
        /// Construct a tree from given storage using specified comparer for keys.
        /// </summary>
        /// <param name="keySerializer">Tool to serialize node keys.</param>
        /// <param name="valueSerializer">Tool to serialize node values.</param>
        /// <param name="recordStorage">Underlying tool for node storage.</param>
        /// <param name="keyComparer">Key comparer.</param>
        public TreeDiskNodeManager(
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer,
            IRecordStorage recordStorage,
            IComparer<K> keyComparer
        )
        {
            if (recordStorage == null)
            {
                throw new ArgumentNullException("nodeStorage");
            }

            this._serializer = new TreeDiskNodeSerializer<K, V>(
                nodeManager: this,
                keySerializer: keySerializer,
                valueSerializer: valueSerializer
            );
            this._recordStorage = recordStorage;
            this.KeyComparer = keyComparer;
            this.EntryComparer = Comparer<Tuple<K, V>>.Create((a, b) =>
                { return KeyComparer.Compare(x: a.Item1, y: b.Item1); });

            // The first record of nodeStorage contains ID of root node.
            // If this record doesn't currently exist, attempt to create it.
            var firstBlockData = recordStorage.Find(1u);

            if (firstBlockData != null)
            {
                this._rootNode = Find(BufferHelper.ReadBufferUInt32(buffer: firstBlockData, bufferOffset: 0));
            }
            else
            {
                this._rootNode = CreateFirstRoot();
            }
        }
        #endregion Constructors

        #region Methods (public)
        public TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIds)
        {
            TreeNode<K, V> node = null;

            _recordStorage.Create(nodeId =>
            {
                node = new TreeNode<K, V>(
                    nodeManager: this,
                    id: nodeId,
                    parentId: 0,
                    entries: entries,
                    childrenIds: childrenIds
                    );
                OnNodeInitialized(node);

                return this._serializer.Serialize(node);
            });

            if (node == null)
            {
                throw new Exception(CommonResources.GetErrorMessage("FailedToCallDataGenerator"));
            }

            return node;
        }

        public TreeNode<K, V> Find(uint id)
        {
            // Check if the node is being held in memory, return it if true.
            if (_nodeWeakRefs.ContainsKey(id))
            {
                TreeNode<K, V> node;

                if (_nodeWeakRefs[id].TryGetTarget(out node))
                {
                    return node;
                }
                else
                { 
                    // Node deallocated, remove weak reference.
                    _nodeWeakRefs.Remove(id);
                }
            }

            // If node note in memory, get it.
            byte[] data = _recordStorage.Find(id);

            if (data == null)
            {
                return null;
            }

            var deserializedNode = this._serializer.Deserialize(assignId: id, record: data);

            OnNodeInitialized(deserializedNode);
            
            return deserializedNode;
        }

        public TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeId, uint rightNodeId)
        {
            var node = Create(
                entries: new Tuple<K, V>[] { new Tuple<K, V>(item1: key, item2: value) }, 
                childrenIds: new uint[] { leftNodeId, rightNodeId }
                );

            this._rootNode = node;
            _recordStorage.Update(recordId: 1u, data: LittleEndianByteOrder.GetBytes(node.Id));

            return this._rootNode;
        }

        public void MakeRoot(TreeNode<K, V> node)
        {
            this._rootNode = node;
            _recordStorage.Update(recordId: 1u, data: LittleEndianByteOrder.GetBytes(node.Id));
        }

        public void Delete(TreeNode<K, V> node)
        {
            if (node == _rootNode)
            {
                _rootNode = null;
            }

            _recordStorage.Delete(node.Id);

            if (_dirtyNodes.ContainsKey(node.Id))
            {
                _dirtyNodes.Remove(node.Id);
            }
        }

        public void MarkAsChanged(TreeNode<K, V> node)
        {
            if (!_dirtyNodes.ContainsKey(node.Id))
            {
                _dirtyNodes.Add(key: node.Id, value: node);
            }
        }

        public void SaveChanges()
        {
            foreach (var kv in _dirtyNodes)
            {
                _recordStorage.Update(recordId: kv.Value.Id, data: this._serializer.Serialize(kv.Value));
            }

            _dirtyNodes.Clear();
        }
        #endregion Methods (public)

        #region Methods (private)
        private TreeNode<K, V> CreateFirstRoot()
        {
            // Write down the ID of first node into the first block.
            _recordStorage.Create(LittleEndianByteOrder.GetBytes((uint)2));

            // Newely created node should have ID = 2.
            return Create(entries: null, childrenIds: null);
        }

        private void OnNodeInitialized(TreeNode<K, V> node)
        {
            // Keep a weak reference to the provided node.
            _nodeWeakRefs.Add(key: node.Id, value: new WeakReference<TreeNode<K, V>>(node));

            // Keep a stron reference to prevent the weak one from being deallocated.
            _nodeStrongRefs.Enqueue(node);

            // Clean up strong refs if there are too many of them.
            if (_nodeStrongRefs.Count >= _maxStrongNodeRefs)
            {
                while (_nodeStrongRefs.Count >= (_maxStrongNodeRefs / 2f))
                {
                    _nodeStrongRefs.Dequeue();
                }
            }

            // Clean up weak refs
            if (this._cleanupCounter++ >= 1000)
            {
                this._cleanupCounter = 0;
                var toBeDeleted = new List<uint>();

                foreach (var kv in this._nodeWeakRefs)
                {
                    TreeNode<K, V> target;

                    if (!kv.Value.TryGetTarget(out target))
                    {
                        toBeDeleted.Add(kv.Key);
                    }
                }

                foreach (var key in toBeDeleted)
                {
                    this._nodeWeakRefs.Remove(key);
                }
            }
        }
        #endregion Methods (private)
    }
}