using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic.Tree
{
    public class TreeNode<K, V>
    {
        #region Variables
        protected uint id = 0;
        protected uint parentId;
        protected readonly ITreeNodeManager<K, V> nodeManager;
        protected readonly List<uint> childrenIds;
        protected readonly List<Tuple<K, V>> entries;
        #endregion Variables

        #region Properties
        public K MaxKey
        {
            get { return entries[entries.Count - 1].Item1; }
        }

        public K MinKey
        {
            get { return entries[0].Item1; }
        }
        
        public bool IsEmpty
        {
            get { return entries.Count == 0; }
        }

        public bool IsLeaf
        {
            get { return childrenIds.Count == 0; }
        }

        public bool IsOverflow
        {
            get { return entries.Count > (nodeManager.MinEntriesPerNode * 2); }
        }

        public int EntriesCount
        {
            get { return entries.Count; }
        }

        public int ChildrenNodeCount
        {
            get { return childrenIds.Count; }
        }

        public uint ParentId
        {
            get { return parentId; }
            private set 
            {
                parentId = value;
                nodeManager.MarkAsChanged(this);
            }
        }

        public uint[] ChildrenIds
        {
            get { return childrenIds.ToArray(); }
        }

        public Tuple<K, V>[] Entries
        {
            get { return entries.ToArray(); }
        }

        /// <summary>
        ///  Id of this node, assigned by node manager.
        ///  Never changed by node itself.
        /// </summary>
        public uint Id
        { get { return id; } }
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

            this.id = id;
            this.parentId = parentId;
            this.nodeManager = nodeManager;
            this.entries = new List<Tuple<K, V>>(this.nodeManager.MinEntriesPerNode * 2);
            this.childrenIds = new List<uint>();

            // Loading data
            if (entries != null)
            {
                this.entries.AddRange(entries);
            }

            if (childrenIds != null)
            {
                this.childrenIds.AddRange(childrenIds);
            }
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Remove an entry from this instance.
        /// </summary>
        public void Remove(int removeAt)
        {
            if (removeAt < 0 || removeAt > this.entries.Count)
            {
                throw new ArgumentOutOfRangeException();
            }

            // If this is a leaf node, flagged entry will be removed,
            // otherwise replace it with the largest value in its left subtree
            // before deleting it from its previous node.
            if (IsLeaf)
            {
                entries.RemoveAt(removeAt);
                nodeManager.MarkAsChanged(this);

                // Check for underflow and Rebalance() if needed
                if ((EntriesCount >= nodeManager.MinEntriesPerNode) || parentId == 0)
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
                var leftSubTree = nodeManager.Find(this.childrenIds[removeAt]);
                TreeNode<K, V> largestNode;
                int largestIndex;

                leftSubTree.FindLargest(node: out largestNode, index: out largestIndex);

                var replacementEntry = largestNode.GetEntry(largestIndex);

                // Replace and remove it from original node
                this.entries[removeAt] = replacementEntry;
                nodeManager.MarkAsChanged(this);
                largestNode.Remove(largestIndex);
            }
        }

        /// <summary>
        /// Get this node's index in its parent.
        /// </summary>
        public int IndexInParent()
        {
            var parent = nodeManager.Find(parentId);

            if (parent == null)
            {
                throw new Exception(CommonResources.GetErrorMessage("FailedToFindParentNode") + Id);
            }

            var childrenIds = parent.childrenIds;

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
                index = this.entries.Count - 1;
                return;
            }
            else
            {
                var rightMostNode = nodeManager.Find(this.childrenIds[this.childrenIds.Count - 1]);
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
                var leftMostNode = nodeManager.Find(this.childrenIds[0]);
                leftMostNode.FindSmallest(node: out node, index: out index);
            }
        }

        public void InsertAsLeaf(K key, V value, int insertPosition)
        {
            entries.Insert(index: insertPosition, item: new Tuple<K, V>(item1: key, item2: value));
            nodeManager.MarkAsChanged(this);
        }

        public void InsertAsParent(K key, V value, uint leftReference, uint rightReference, out int insertPosition)
        {
            // Find insert position
            insertPosition = BinarySearchEntriesForKey(key);
            insertPosition = (insertPosition >= 0) ? insertPosition : ~insertPosition;

            // Insert entry
            entries.Insert(index: insertPosition, item: new Tuple<K, V>(item1: key, item2: value));

            // Insert and update child references
            childrenIds.Insert(index: insertPosition, item: leftReference);
            childrenIds[insertPosition + 1] = rightReference;

            nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Split node in half.
        /// </summary>
        public void Split(out TreeNode<K, V> outLeftNode, out TreeNode<K, V> outRightNode)
        {
            ushort halfCount = this.nodeManager.MinEntriesPerNode;
            var middleEntry = entries[halfCount];

            // Create a new node to hold all values larger then the middle one
            var rightEntries = new Tuple<K, V>[halfCount];
            uint[] rightChildren = null;

            entries.CopyTo(
                index: halfCount + 1,
                array: rightEntries,
                arrayIndex: 0,
                count: rightEntries.Length
            );

            if (!IsLeaf)
            {
                rightChildren = new uint[halfCount + 1];
                childrenIds.CopyTo(
                    index: halfCount + 1,
                    array: rightChildren,
                    arrayIndex: 0,
                    count: rightChildren.Length
                );
            }

            var newRightNode = nodeManager.Create(entries: rightEntries, childrenIds: rightChildren);

            // Update ParentID property for moved children nodes
            if (rightChildren != null)
            {
                foreach (uint childId in rightChildren)
                {
                    nodeManager.Find(childId).ParentId = newRightNode.Id;
                }
            }

            // Remove all values after the halfCount from current node
            entries.RemoveRange(halfCount);

            if (!IsLeaf)
            {
                childrenIds.RemoveRange(halfCount + 1);
            }

            // Insert middle element to parent node or
            // make it into a new root if there is no parent
            var parent = (parentId == 0) ? null : nodeManager.Find(parentId);

            if (parent == null)
            {
                parent = this.nodeManager.CreateNewRoot(
                    key: middleEntry.Item1,
                    value: middleEntry.Item2,
                    leftNodeId: Id,
                    rightNodeId: newRightNode.Id
                );

                this.ParentId = parent.Id;
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

            nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Perform binary search on entries
        /// </summary>
        public int BinarySearchEntriesForKey(K key)
        {
            return entries.BinarySearch(
                item: new Tuple<K, V>(item1: key, item2: default(V)),
                comparer: this.nodeManager.EntryComparer
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
                return entries.BinarySearchFirst(
                    value: new Tuple<K, V>(item1: key, item2: default(V)),
                    comparer: this.nodeManager.EntryComparer
                );
            }
            else
            {
                return entries.BinarySearchLast(
                    value: new Tuple<K, V>(item1: key, item2: default(V)),
                    comparer: this.nodeManager.EntryComparer
                );
            }
        }

        /// <summary>
        /// Get a child node by its internal position to this node.
        /// </summary>
        public TreeNode<K, V> GetChildNode(int index)
        {
            return nodeManager.Find(childrenIds[index]);
        }

        /// <summary>
        /// Get a Key-Value entry inside this node.
        /// </summary>
        public Tuple<K, V> GetEntry(int index)
        {
            return entries[index];
        }

        /// <summary>
        /// Check if entry at specified index exists.
        /// </summary>
        public bool EntryExists(int index)
        {
            return index < entries.Count;
        }

        public override string ToString()
        {
            string[] numbers = (from tuple in this.entries select tuple.Item1.ToString()).ToArray();

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
                var ids = (from id in this.childrenIds select id.ToString()).ToArray();
               
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
            var parent = nodeManager.Find(parentId);
            var rightSibling = ((indexInParent + 1) < parent.ChildrenNodeCount) ? parent.GetChildNode(indexInParent + 1) : null;

            if (rightSibling != null && rightSibling.EntriesCount > nodeManager.MinEntriesPerNode)
            {
                // Copy the separator from parent to deficient node.
                // Separator moves down, deficient node now has the minimum number of elements.
                entries.Add(parent.GetEntry(indexInParent));

                // Replace the separator in parent with the first element of the right sibling.
                // Right sibling loses one node, still contains at least the minimum number of elements.
                parent.entries[indexInParent] = rightSibling.entries[0];
                rightSibling.entries.RemoveAt(0);

                // Move the first child reference from right sibling.
                if (!rightSibling.IsLeaf)
                {
                    var node = nodeManager.Find(rightSibling.childrenIds[0]);

                    node.parentId = this.Id;
                    nodeManager.MarkAsChanged(node);

                    childrenIds.Add(rightSibling.childrenIds[0]);
                    rightSibling.childrenIds.RemoveAt(0);
                }

                nodeManager.MarkAsChanged(this);
                nodeManager.MarkAsChanged(parent);
                nodeManager.MarkAsChanged(rightSibling);
                return;
            }

            // If deficient node's left sibling exists and has more than minimum
            // number of elements, rotate right.
            var leftSibling = ((indexInParent - 1) >= 0) ? parent.GetChildNode(indexInParent - 1) : null;

            if (leftSibling != null && leftSibling.EntriesCount > nodeManager.MinEntriesPerNode)
            {
                // Copy the separator from parent to the start of deficient node.
                // Separator moves down, deficient node now has the minimum number of elements.
                entries.Insert(index: 0, item: parent.GetEntry(indexInParent - 1));

                // Replace the separator in parent with the last element of the left sibling.
                // Left sibling loses one node, still contains at least the minimum number of elements.
                parent.entries[indexInParent - 1] = leftSibling.entries[leftSibling.entries.Count - 1];
                leftSibling.entries.RemoveAt(leftSibling.entries.Count - 1);

                // Move the last child reference from left sibling.
                if (!leftSibling.IsLeaf)
                {
                    var node = nodeManager.Find(leftSibling.childrenIds[leftSibling.entries.Count - 1]);

                    node.parentId = this.Id;
                    nodeManager.MarkAsChanged(node);

                    childrenIds.Insert(index: 0, item: leftSibling.childrenIds[leftSibling.entries.Count - 1]);
                    leftSibling.childrenIds.RemoveAt(leftSibling.entries.Count - 1);
                }

                nodeManager.MarkAsChanged(this);
                nodeManager.MarkAsChanged(parent);
                nodeManager.MarkAsChanged(leftSibling);
                return;
            }

            // If both immediate siblings have only the minimum number of elements,
            // merge with a sibling sandwiching their separator taken from their parent.
            var leftChild = (rightSibling != null) ? this : leftSibling;
            var rightChild = (rightSibling != null) ? rightSibling : this;
            var parentSeparatorIndex = (rightSibling != null) ? indexInParent : (indexInParent - 1);

            // Copy the separator to the end of left node
            leftChild.entries.Add(parent.GetEntry(parentSeparatorIndex));

            // Move all elements from right node to the left one.
            leftChild.entries.AddRange(rightChild.entries);
            leftChild.childrenIds.AddRange(rightChild.childrenIds);

            foreach (var id in rightChild.childrenIds)
            {
                var n = nodeManager.Find(id);

                n.parentId = leftChild.Id;
                nodeManager.MarkAsChanged(n);
            }

            // Remove separator and the empty right child from the parent.
            parent.entries.RemoveAt(parentSeparatorIndex);
            parent.childrenIds.RemoveAt(parentSeparatorIndex + 1);
            nodeManager.Delete(rightChild);

            // If parent is the root and has no elements,
            // free it and make the merge node into the new root.
            // Otherwise, if parent has fewer than required number of elements, rebalance.
            if (parent.parentId == 0 && parent.EntriesCount == 0)
            {
                leftChild.parentId = 0;
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MakeRoot(leftChild);
                nodeManager.Delete(parent);
            }
            else if (parent.parentId != 0 && parent.EntriesCount < nodeManager.MinEntriesPerNode)
            {
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MarkAsChanged(parent);
                parent.Rebalance();
            }
            else
            {
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MarkAsChanged(parent);
            }
        }
        #endregion Methods (private)
    }
}