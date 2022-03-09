using CustomDatabase.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CustomDatabase.Logic.Tree
{
    public class TreeNode<K, V>
    {
        #region Variables
        protected uint id = 0;
        protected uint parentID;
        protected readonly ITreeNodeManager<K, V> nodeManager;
        protected readonly List<uint> childrenIDs;
        protected readonly List<Tuple<K, V>> entries;
        #endregion

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
            get { return childrenIDs.Count == 0; }
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
            get { return childrenIDs.Count; }
        }

        public uint ParentID
        {
            get { return parentID; }
            private set 
            {
                parentID = value;
                nodeManager.MarkAsChanged(this);
            }
        }

        public uint[] ChildrenIDs
        {
            get { return childrenIDs.ToArray(); }
        }

        public Tuple<K, V>[] Entries
        {
            get { return entries.ToArray(); }
        }

        /// <summary>
        ///  Id of this node, assigned by node manager.
        ///  Never changed by node itself.
        /// </summary>
        public uint ID
        { get { return id; } }
        #endregion

        #region Constructor
        /// <summary>
        /// Initializes a new instance of the TreeNode class.
        /// </summary>
        /// <param name="branchingFactor">Branching factor</param>
        /// <param name="nodeManager">Node manager.</param>
        public TreeNode(ITreeNodeManager<K, V> nodeManager, 
            uint ID, 
            uint parentID, 
            IEnumerable<Tuple<K, V>> entries = null,
            IEnumerable<uint> childrenIDs = null)
        {
            if (nodeManager == null)
            { throw new ArgumentNullException(nameof(nodeManager)); }

            this.id = ID;
            this.parentID = parentID;
            this.nodeManager = nodeManager;
            this.entries = new List<Tuple<K, V>>(this.nodeManager.MinEntriesPerNode * 2);
            this.childrenIDs = new List<uint>();

            // Loading data
            if (entries != null)
            { this.entries.AddRange(entries); }

            if (childrenIDs != null)
            { this.childrenIDs.AddRange(childrenIDs); }
        }
        #endregion

        #region Methods (public)
        /// <summary>
        /// Remove an entry from this instance.
        /// </summary>
        public void Remove(int removeAt)
        {
            if (removeAt < 0 || removeAt > this.entries.Count)
            { throw new ArgumentOutOfRangeException(); }

            // If this is a leaf node, flagged entry will be removed,
            // otherwise replace it with the largest value in its left subtree
            // before deleting it from its previous node.
            if (IsLeaf)
            {
                entries.RemoveAt(removeAt);
                nodeManager.MarkAsChanged(this);

                // Check for underflow and Rebalance() if needed
                if ((EntriesCount >= nodeManager.MinEntriesPerNode) || parentID == 0)
                { return; }
                else
                { Rebalance(); }
            }
            else
            {
                // Find largest entry on the left subtree
                var leftSubTree = nodeManager.Find(this.childrenIDs[removeAt]);
                TreeNode<K, V> largestNode;
                int largestIndex;

                leftSubTree.FindLargest(out largestNode, out largestIndex);

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
            var parent = nodeManager.Find(parentID);

            if (parent == null)
            { throw new Exception("IndexInParent failed to find parent node of " + ID); }

            var childrenIDs = parent.childrenIDs;

            for (int i = 0; i < childrenIDs.Count; i++)
            {
                if (childrenIDs[i] == ID)
                { return i; }
            }

            throw new Exception("Failed to find index of node " + ID + "in its parent.");
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
                var rightMostNode = nodeManager.Find(this.childrenIDs[this.childrenIDs.Count - 1]);
                rightMostNode.FindLargest(out node, out index);
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
                var leftMostNode = nodeManager.Find(this.childrenIDs[0]);
                leftMostNode.FindSmallest(out node, out index);
            }
        }

        public void InsertAsLeaf(K key, V value, int insertPosition)
        {
            entries.Insert(insertPosition, new Tuple<K, V>(key, value));
            nodeManager.MarkAsChanged(this);
        }

        public void InsertAsParent(K key, V value, uint leftReference, uint rightReference, out int insertPosition)
        {
            // Find insert position
            insertPosition = BinarySearchEntriesForKey(key);
            insertPosition = (insertPosition >= 0) ? insertPosition : ~insertPosition;

            // Insert entry
            entries.Insert(insertPosition, new Tuple<K, V>(key, value));

            // Insert and update child references
            childrenIDs.Insert(insertPosition, leftReference);
            childrenIDs[insertPosition + 1] = rightReference;

            nodeManager.MarkAsChanged(this);
        }

        /// <summary>
        /// Split node in half.
        /// </summary>
        public void Split(out TreeNode<K, V> outLeftNode, out TreeNode<K, V> outRightNode)
        {
            var halfCount = this.nodeManager.MinEntriesPerNode;
            var middleEntry = entries[halfCount];

            // Create a new node to hold all values larger then the middle one
            var rightEntries = new Tuple<K, V>[halfCount];
            var rightChildren = (uint[])null;

            entries.CopyTo(halfCount + 1, rightEntries, 0, rightEntries.Length);

            if (!IsLeaf)
            {
                rightChildren = new uint[halfCount + 1];
                childrenIDs.CopyTo(halfCount + 1, rightChildren, 0, rightChildren.Length);
            }

            var newRightNode = nodeManager.Create(rightEntries, rightChildren);

            // Update ParentID property for moved children nodes
            if (rightChildren != null)
            {
                foreach (var childID in rightChildren)
                { nodeManager.Find(childID).ParentID = newRightNode.ID; }
            }

            // Remove all values after the halfCount from current node
            entries.RemoveRange(halfCount);

            if (!IsLeaf)
            { childrenIDs.RemoveRange(halfCount + 1); }

            // Insert middle element to parent node or
            // make it into a new root if there is no parent
            var parent = (parentID == 0) ? null : nodeManager.Find(parentID);

            if (parent == null)
            {
                parent = this.nodeManager.CreateNewRoot(
                    middleEntry.Item1,
                    middleEntry.Item2,
                    ID,
                    newRightNode.ID);

                this.ParentID = parent.ID;
                newRightNode.ParentID = parent.ID;
            }
            else
            {
                int insertPosition;

                parent.InsertAsParent(
                    middleEntry.Item1,
                    middleEntry.Item2,
                    ID,
                    newRightNode.ID,
                    out insertPosition);

                newRightNode.ParentID = parent.ID;

                // If parent is overflow, split and update reference
                if (parent.IsOverflow)
                {
                    TreeNode<K, V> left, right;
                    parent.Split(out left, out right);
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
            return entries.BinarySearch(new Tuple<K, V>(key, default(V)), this.nodeManager.EntryComparer);
        }

        /// <summary>
        /// Perform binary search on entries but return first or last
        /// one in case of multiplu occurences.
        /// </summary>
        /// <param name="firstOccurence">Whether to return the first or last of the found entries.</param>
        public int BinarySearchEntriesForKey(K key, bool firstOccurence)
        {
            if (firstOccurence)
            { return entries.BinarySearchFirst(new Tuple<K, V>(key, default(V)), this.nodeManager.EntryComparer); }
            else
            { return entries.BinarySearchLast(new Tuple<K, V>(key, default(V)), this.nodeManager.EntryComparer); }
        }

        /// <summary>
        /// Get a child node by its internal position to this node.
        /// </summary>
        public TreeNode<K, V> GetChildNode(int atIndex)
        {
            return nodeManager.Find(childrenIDs[atIndex]);
        }

        /// <summary>
        /// Get a Key-Value entry inside this node.
        /// </summary>
        public Tuple<K, V> GetEntry(int atIndex)
        {
            return entries[atIndex];
        }

        /// <summary>
        /// Check if entry at specified index exists.
        /// </summary>
        public bool EntryExists(int atIndex)
        {
            return atIndex < entries.Count;
        }

        public override string ToString()
        {
            var numbers = (from tuple in this.entries select tuple.Item1.ToString()).ToArray();

            if (IsLeaf)
            {
                return string.Format("[Node: ID={0}, ParentID={1}, Entries={2}]",
                    ID,
                    ParentID,
                    String.Join(",", numbers)
                    );
            }
            else
            {
                var IDs = (from id in this.childrenIDs select id.ToString()).ToArray();
               
                return string.Format("[Node: ID={0}, ParentID={1}, Entries={2}, Children={3}]",
                    ID,
                    ParentID,
                    String.Join(",", numbers),
                    String.Join(",", IDs)
                    );
            }
        }
        #endregion

        #region Methods (private)
        /// <summary>
        /// Rebalance this node after an element was removed, causing underflow.
        /// </summary>
        void Rebalance()
        {
            // If deficient node's right sibling exists and has more than minimum
            // number of elements, rotate left.
            var indexInParent = IndexInParent();
            var parent = nodeManager.Find(parentID);
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
                    var n = nodeManager.Find(rightSibling.childrenIDs[0]);

                    n.parentID = this.ID;
                    nodeManager.MarkAsChanged(n);

                    childrenIDs.Add(rightSibling.childrenIDs[0]);
                    rightSibling.childrenIDs.RemoveAt(0);
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
                entries.Insert(0, parent.GetEntry(indexInParent - 1));

                // Replace the separator in parent with the last element of the left sibling.
                // Left sibling loses one node, still contains at least the minimum number of elements.
                parent.entries[indexInParent - 1] = leftSibling.entries[leftSibling.entries.Count - 1];
                leftSibling.entries.RemoveAt(leftSibling.entries.Count - 1);

                // Move the last child reference from left sibling.
                if (!leftSibling.IsLeaf)
                {
                    var n = nodeManager.Find(leftSibling.childrenIDs[leftSibling.entries.Count - 1]);

                    n.parentID = this.ID;
                    nodeManager.MarkAsChanged(n);

                    childrenIDs.Insert(0, leftSibling.childrenIDs[leftSibling.entries.Count - 1]);
                    leftSibling.childrenIDs.RemoveAt(leftSibling.entries.Count - 1);
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
            leftChild.childrenIDs.AddRange(rightChild.childrenIDs);

            foreach (var id in rightChild.childrenIDs)
            {
                var n = nodeManager.Find(id);

                n.parentID = leftChild.ID;
                nodeManager.MarkAsChanged(n);
            }

            // Remove separator and the empty right child from the parent.
            parent.entries.RemoveAt(parentSeparatorIndex);
            parent.childrenIDs.RemoveAt(parentSeparatorIndex + 1);
            nodeManager.Delete(rightChild);

            // If parent is the root and has no elements,
            // free it and make the merge node into the new root.
            // Otherwise, if parent has fewer than required number of elements, rebalance.
            if (parent.parentID == 0 && parent.EntriesCount == 0)
            {
                leftChild.parentID = 0;
                nodeManager.MarkAsChanged(leftChild);
                nodeManager.MakeRoot(leftChild);
                nodeManager.Delete(parent);
            }
            else if (parent.parentID != 0 && parent.EntriesCount < nodeManager.MinEntriesPerNode)
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
        #endregion
    }
}
