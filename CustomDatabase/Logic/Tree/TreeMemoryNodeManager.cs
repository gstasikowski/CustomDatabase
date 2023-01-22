using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class TreeMemoryNodeManager<K, V> : ITreeNodeManager<K, V>
    {
        #region Variables
        private readonly Dictionary<uint, TreeNode<K, V>> _nodes = new Dictionary<uint, TreeNode<K, V>>();
        private readonly ushort _minEntriesCountPerNode;
        private readonly IComparer<K> _keyComparer;
        private readonly IComparer<Tuple<K, V>> _entryComparer;

        private int _idCounter = 1;
        private TreeNode<K, V> _rootNode;
        #endregion Variables

        #region Properties
        public ushort MinEntriesPerNode
        {
            get { return _minEntriesCountPerNode; }
        }

        public IComparer<K> KeyComparer
        {
            get { return _keyComparer; }
        }

        public IComparer<Tuple<K, V>> EntryComparer
        {
            get { return _entryComparer; }
        }

        public TreeNode<K, V> RootNode
        {
            get { return _rootNode; }
        }
        #endregion Properties

        #region Constructor
        /// <param name="minEntriesCountPerNode">Multiplied by 2 is the degree of the tree.</param>
        /// <param name="keyComparer">Key comparer.</param>
        public TreeMemoryNodeManager(ushort minEntriesCountPerNode, IComparer<K> keyComparer)
        {
            _minEntriesCountPerNode = minEntriesCountPerNode;
            _keyComparer = keyComparer;
            _entryComparer = Comparer<Tuple<K, V>>.Create((t1, t2) =>
            { return _keyComparer.Compare(x: t1.Item1, y: t2.Item1); });

            _rootNode = Create(entries: null, childrenIds: null);
        }
        #endregion Constructor

        #region Methods (public)
        public TreeNode<K, V> Create(IEnumerable<Tuple<K, V>> entries, IEnumerable<uint> childrenIds)
        {
            var newNode = new TreeNode<K, V>(
                nodeManager: this,
                id: (uint)(_idCounter++),
                parentId: 0,
                entries: entries,
                childrenIds: childrenIds
            );

            _nodes[newNode.Id] = newNode;

            return newNode;
        }

        public TreeNode<K, V> Find(uint id)
        {
            if (!_nodes.ContainsKey(id))
            {
                throw new ArgumentException(CommonResources.GetErrorMessage("NodeNotFoundById") + id);
            }

            return _nodes[id];
        }

        public TreeNode<K, V> CreateNewRoot(K key, V value, uint leftNodeId, uint rightNodeId)
        {
            var newNode = Create(
                entries: new Tuple<K, V>[] { new Tuple<K, V>(item1: key, item2: value) },
                childrenIds: new uint[] { leftNodeId, rightNodeId }
            );

            _rootNode = newNode;

            return newNode;
        }

        public void Delete(TreeNode<K, V> target)
        {
            if (target == _rootNode)
            {
                _rootNode = null;
            }

            if (_nodes.ContainsKey(target.Id))
            {
                _nodes.Remove(target.Id);
            }
        }

        public void MakeRoot(TreeNode<K, V> target)
        {
            _rootNode = target;
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