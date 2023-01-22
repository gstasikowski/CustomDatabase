using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic
{
    public class TreeNode<K, V>
    {
        #region Variables
        private uint _id = 0;
        private uint _parentId;
        private readonly ITreeNodeManager<K, V> _nodeManager;
        private readonly List<uint> _childrenIds;
        private readonly List<Tuple<K, V>> _entries;
        #endregion Variables

        #region Properties
        public K MaxKey
        {
            get { return _entries[_entries.Count - 1].Item1; }
        }

        public K MinKey
        {
            get { return _entries[0].Item1; }
        }
        
        public bool IsEmpty
        {
            get { return _entries.Count == 0; }
        }

        public bool IsLeaf
        {
            get { return _childrenIds.Count == 0; }
        }

        public bool IsOverflow
        {
            get { return _entries.Count > (_nodeManager.MinEntriesPerNode * 2); }
        }

        public int EntriesCount
        {
            get { return _entries.Count; }
        }

        public int ChildrenNodeCount
        {
            get { return _childrenIds.Count; }
        }

        public uint ParentId
        {
            get { return _parentId; }
            private set 
            {
                _parentId = value;
                _nodeManager.MarkAsChanged(this);
            }
        }

        public uint[] ChildrenIds
        {
            get { return _childrenIds.ToArray(); }
        }

        public Tuple<K, V>[] Entries
        {
            get { return _entries.ToArray(); }
        }

        /// <summary>
        ///  Id of this node, assigned by node manager.
        ///  Never changed by node itself.
        /// </summary>
        public uint Id
        { get { return _id; } }
        #endregion

        #region Constructor
        /// <summary>
        /// Initializes a new instance of the TreeNode class.
        /// </summary>
        /// <param name="branchingFactor">Branching factor</param>
        /// <param name="nodeManager">Node manager.</param>
        public TreeNode(
            ITreeNodeManager<K, V> nodeManager, 
            uint id, 
            uint parentId, 
            IEnumerable<Tuple<K, V>> entries = null,
            IEnumerable<uint> childrenIds = null
        )
        {
            if (nodeManager == null)
            {
                throw new ArgumentNullException(nameof(nodeManager));
            }

            _id = id;
            _parentId = parentId;
            _nodeManager = nodeManager;
            _entries = new List<Tuple<K, V>>(_nodeManager.MinEntriesPerNode * 2);
            _childrenIds = new List<uint>();

            // Loading data
            if (entries != null)
            {
                _entries.AddRange(entries);
            }

            if (childrenIds != null)
            {
                _childrenIds.AddRange(childrenIds);
            }
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Remove an entry from this instance.
        /// </summary>
        public void Remove(int removeAt)
        {
            if (removeAt < 0 || removeAt > _entries.Count)
            {
                throw new ArgumentOutOfRangeException();
            }

            // If this is a leaf node, flagged entry will be removed,
            // otherwise replace it with the largest value in its left subtree
            // before deleting it from its previous node.
            if (IsLeaf)
            {
                _entries.RemoveAt(removeAt);
                _nodeManager.MarkAsChanged(this);

                // Check for underflow and Rebalance() if needed
                if ((EntriesCount >= _nodeManager.MinEntriesPerNode) || _parentId == 0)
                {
                    return;
                }
                else
                {
                    Rebalance();
                }
            }
            else
            {
                // Find largest entry on the left subtree
                var leftSubTree = _nodeManager.Find(_childrenIds[removeAt]);
                TreeNode<K, V> largestNode;
                int largestIndex;

                leftSubTree.FindLargest(node: out largestNode, index: out largestIndex);

                var replacementEntry = largestNode.GetEntry(largestIndex);

                // Replace and remove it from original node
                _entries[removeAt] = replacementEntry;
                _nodeManager.MarkAsChanged(this);
                largestNode.Remove(largestIndex);
            }
        }

        /// <summary>
        /// Get this node's index in its parent.
        /// </summary>
        public int IndexInParent()
        {
            var parent = _nodeManager.Find(_parentId);

            if (parent == null)
            {
                throw new Exception(CommonResources.GetErrorMessage("FailedToFindParentNode") + Id);
            }

            var childrenIds = parent._childrenIds;

            for (int index = 0; index < childrenIds.Count; index++)
            {
                if (childrenIds[index] == Id)
                {
                    return index;
                }
            }

            throw new Exception(CommonResources.GetErrorMessage("FailedToFindIndexOfNodeInParent"));
        }

        /// <summary>
        /// Find the largest entry on this subtree and output it to specified parameters.
        /// </summary>
        public void FindLargest(out TreeNode<K, V> node, out int index)
        {
            // If node is a leaf return its max value (we've reached the bottom a the tree),
            // otherwise keep going (down and to the right).
            if (IsLeaf)
            {
                node = this;
                index = _entries.Count - 1;
                return;
            }
            else
            {
                var rightMostNode = _nodeManager.Find(_childrenIds[_childrenIds.Count - 1]);
                rightMostNode.FindLargest(node: out node, index: out index);
            }
        }

        /// <summary>
        /// Find the smallest entry on this subtree and output it to specified parameters.
        /// </summary>
        public void FindSmallest(out TreeNode<K, V> node, out int index)
        {
            // Same situation as above, except down and to the left
            if (IsLeaf)
            {
                node = this;
                index = 0;
                return;
            }
            else
            {
                var leftMostNode = _nodeManager.Find(_childrenIds[0]);
                leftMostNode.FindSmallest(node: out node, index: out index);
            }
        }

        public void InsertAsLeaf(K key, V value, int insertPosition)
        {
            _entries.Insert(index: insertPosition, item: new Tuple<K, V>(item1: key, item2: value));
            _nodeManager.MarkAsChanged(this);
        }

        public void InsertAsParent(K key, V value, uint leftReference, uint rightReference, out int insertPosition)
        {
            // Find insert position
            insertPosition = BinarySearchEntriesForKey(key);
            insertPosition = (insertPosition >= 0) ? insertPosition : ~insertPosition;

            // Insert entry
            _entries.Insert(index: insertPosition, item: new Tuple<K, V>(item1: key, item2: value));

            // Insert and update child references
            _childrenIds.Insert(index: insertPosition, item: leftReference);
            _childrenIds[insertPosition + 1] = rightReference;

            _nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Split node in half.
        /// </summary>
        public void Split(out TreeNode<K, V> outLeftNode, out TreeNode<K, V> outRightNode)
        {
            ushort halfCount = _nodeManager.MinEntriesPerNode;
            var middleEntry = _entries[halfCount];

            // Create a new node to hold all values larger then the middle one
            var rightEntries = new Tuple<K, V>[halfCount];
            uint[] rightChildren = null;

            _entries.CopyTo(
                index: halfCount + 1,
                array: rightEntries,
                arrayIndex: 0,
                count: rightEntries.Length
            );

            if (!IsLeaf)
            {
                rightChildren = new uint[halfCount + 1];
                _childrenIds.CopyTo(
                    index: halfCount + 1,
                    array: rightChildren,
                    arrayIndex: 0,
                    count: rightChildren.Length
                );
            }

            var newRightNode = _nodeManager.Create(entries: rightEntries, childrenIds: rightChildren);

            // Update ParentID property for moved children nodes
            if (rightChildren != null)
            {
                foreach (uint childId in rightChildren)
                {
                    _nodeManager.Find(childId).ParentId = newRightNode.Id;
                }
            }

            // Remove all values after the halfCount from current node
            _entries.RemoveRange(halfCount);

            if (!IsLeaf)
            {
                _childrenIds.RemoveRange(halfCount + 1);
            }

            // Insert middle element to parent node or
            // make it into a new root if there is no parent
            var parent = (_parentId == 0) ? null : _nodeManager.Find(_parentId);

            if (parent == null)
            {
                parent = _nodeManager.CreateNewRoot(
                    key: middleEntry.Item1,
                    value: middleEntry.Item2,
                    leftNodeId: Id,
                    rightNodeId: newRightNode.Id
                );

                ParentId = parent.Id;
                newRightNode.ParentId = parent.Id;
            }
            else
            {
                int insertPosition;

                parent.InsertAsParent(
                    key: middleEntry.Item1,
                    value: middleEntry.Item2,
                    leftReference: Id,
                    rightReference: newRightNode.Id,
                    insertPosition: out insertPosition
                );

                newRightNode.ParentId = parent.Id;

                // If parent is overflow, split and update reference
                if (parent.IsOverflow)
                {
                    TreeNode<K, V> left, right;
                    parent.Split(outLeftNode: out left, outRightNode: out right);
                }
            }

            outLeftNode = this;
            outRightNode = newRightNode;

            _nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Perform binary search on entries
        /// </summary>
        public int BinarySearchEntriesForKey(K key)
        {
            return _entries.BinarySearch(
                item: new Tuple<K, V>(item1: key, item2: default(V)),
                comparer: _nodeManager.EntryComparer
            );
        }

        /// <summary>
        /// Perform binary search on entries but return first or last
        /// one in case of multiplu occurences.
        /// </summary>
        /// <param name="firstOccurence">Whether to return the first or last of the found entries.</param>
        public int BinarySearchEntriesForKey(K key, bool firstOccurence)
        {
            if (firstOccurence)
            {
                return _entries.BinarySearchFirst(
                    value: new Tuple<K, V>(item1: key, item2: default(V)),
                    comparer: _nodeManager.EntryComparer
                );
            }
            else
            {
                return _entries.BinarySearchLast(
                    value: new Tuple<K, V>(item1: key, item2: default(V)),
                    comparer: _nodeManager.EntryComparer
                );
            }
        }

        /// <summary>
        /// Get a child node by its internal position to this node.
        /// </summary>
        public TreeNode<K, V> GetChildNode(int index)
        {
            return _nodeManager.Find(_childrenIds[index]);
        }

        /// <summary>
        /// Get a Key-Value entry inside this node.
        /// </summary>
        public Tuple<K, V> GetEntry(int index)
        {
            return _entries[index];
        }

        /// <summary>
        /// Check if entry at specified index exists.
        /// </summary>
        public bool EntryExists(int index)
        {
            return index < _entries.Count;
        }

        public override string ToString()
        {
            string[] numbers = (from tuple in _entries select tuple.Item1.ToString()).ToArray();

            if (IsLeaf)
            {
                return string.Format(
                    format: "[Node: ID={0}, ParentID={1}, Entries={2}]",
                    arg0: Id,
                    arg1: ParentId,
                    arg2: String.Join(separator: ",", value: numbers)
                );
            }
            else
            {
                var ids = (from id in _childrenIds select id.ToString()).ToArray();
               
                return string.Format(
                    "[Node: ID={0}, ParentID={1}, Entries={2}, Children={3}]",
                    Id,
                    ParentId,
                    String.Join(separator: ",", value: numbers),
                    String.Join(separator: ",", value: ids)
                );
            }
        }
        #endregion Methods (public)

        #region Methods (private)
        /// <summary>
        /// Rebalance this node after an element was removed, causing underflow.
        /// </summary>
        private void Rebalance()
        {
            // If deficient node's right sibling exists and has more than minimum
            // number of elements, rotate left.
            int indexInParent = IndexInParent();
            var parent = _nodeManager.Find(_parentId);
            var rightSibling = ((indexInParent + 1) < parent.ChildrenNodeCount) ? parent.GetChildNode(indexInParent + 1) : null;

            if (rightSibling != null && rightSibling.EntriesCount > _nodeManager.MinEntriesPerNode)
            {
                // Copy the separator from parent to deficient node.
                // Separator moves down, deficient node now has the minimum number of elements.
                _entries.Add(parent.GetEntry(indexInParent));

                // Replace the separator in parent with the first element of the right sibling.
                // Right sibling loses one node, still contains at least the minimum number of elements.
                parent._entries[indexInParent] = rightSibling._entries[0];
                rightSibling._entries.RemoveAt(0);

                // Move the first child reference from right sibling.
                if (!rightSibling.IsLeaf)
                {
                    var node = _nodeManager.Find(rightSibling._childrenIds[0]);

                    node._parentId = Id;
                    _nodeManager.MarkAsChanged(node);

                    _childrenIds.Add(rightSibling._childrenIds[0]);
                    rightSibling._childrenIds.RemoveAt(0);
                }

                _nodeManager.MarkAsChanged(this);
                _nodeManager.MarkAsChanged(parent);
                _nodeManager.MarkAsChanged(rightSibling);
                return;
            }

            // If deficient node's left sibling exists and has more than minimum
            // number of elements, rotate right.
            var leftSibling = ((indexInParent - 1) >= 0) ? parent.GetChildNode(indexInParent - 1) : null;

            if (leftSibling != null && leftSibling.EntriesCount > _nodeManager.MinEntriesPerNode)
            {
                // Copy the separator from parent to the start of deficient node.
                // Separator moves down, deficient node now has the minimum number of elements.
                _entries.Insert(index: 0, item: parent.GetEntry(indexInParent - 1));

                // Replace the separator in parent with the last element of the left sibling.
                // Left sibling loses one node, still contains at least the minimum number of elements.
                parent._entries[indexInParent - 1] = leftSibling._entries[leftSibling._entries.Count - 1];
                leftSibling._entries.RemoveAt(leftSibling._entries.Count - 1);

                // Move the last child reference from left sibling.
                if (!leftSibling.IsLeaf)
                {
                    var node = _nodeManager.Find(leftSibling._childrenIds[leftSibling._entries.Count - 1]);

                    node._parentId = Id;
                    _nodeManager.MarkAsChanged(node);

                    _childrenIds.Insert(index: 0, item: leftSibling._childrenIds[leftSibling._entries.Count - 1]);
                    leftSibling._childrenIds.RemoveAt(leftSibling._entries.Count - 1);
                }

                _nodeManager.MarkAsChanged(this);
                _nodeManager.MarkAsChanged(parent);
                _nodeManager.MarkAsChanged(leftSibling);
                return;
            }

            // If both immediate siblings have only the minimum number of elements,
            // merge with a sibling sandwiching their separator taken from their parent.
            var leftChild = (rightSibling != null) ? this : leftSibling;
            var rightChild = (rightSibling != null) ? rightSibling : this;
            var parentSeparatorIndex = (rightSibling != null) ? indexInParent : (indexInParent - 1);

            // Copy the separator to the end of left node
            leftChild._entries.Add(parent.GetEntry(parentSeparatorIndex));

            // Move all elements from right node to the left one.
            leftChild._entries.AddRange(rightChild._entries);
            leftChild._childrenIds.AddRange(rightChild._childrenIds);

            foreach (var id in rightChild._childrenIds)
            {
                var n = _nodeManager.Find(id);

                n._parentId = leftChild.Id;
                _nodeManager.MarkAsChanged(n);
            }

            // Remove separator and the empty right child from the parent.
            parent._entries.RemoveAt(parentSeparatorIndex);
            parent._childrenIds.RemoveAt(parentSeparatorIndex + 1);
            _nodeManager.Delete(rightChild);

            // If parent is the root and has no elements,
            // free it and make the merge node into the new root.
            // Otherwise, if parent has fewer than required number of elements, rebalance.
            if (parent._parentId == 0 && parent.EntriesCount == 0)
            {
                leftChild._parentId = 0;
                _nodeManager.MarkAsChanged(leftChild);
                _nodeManager.MakeRoot(leftChild);
                _nodeManager.Delete(parent);
            }
            else if (parent._parentId != 0 && parent.EntriesCount < _nodeManager.MinEntriesPerNode)
            {
                _nodeManager.MarkAsChanged(leftChild);
                _nodeManager.MarkAsChanged(parent);
                parent.Rebalance();
            }
            else
            {
                _nodeManager.MarkAsChanged(leftChild);
                _nodeManager.MarkAsChanged(parent);
            }
        }
        #endregion Methods (private)
    }
}