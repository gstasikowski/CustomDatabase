using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System.Collections;

namespace CustomDatabase.Logic.Tree
{
    public class TreeEnumerator<K, V> : IEnumerator<Tuple<K, V>>
    {
        #region Variables
        private readonly ITreeNodeManager<K, V> nodeManager;
        private readonly TreeTraverseDirection direction;

        private bool isDoneIterating = false;
        private int currentEntry = 0;
        private TreeNode<K, V> currentNode;
        private Tuple<K, V> current;
        #endregion Variables

        #region Properties
        public TreeNode<K, V> CurrentNode
        {
            get { return currentNode; }
        }

        public int CurrentEntry
        {
            get { return currentEntry; }
        }

        object IEnumerator.Current
        {
            get { return (object)Current; }
        }

        public Tuple<K, V> Current
        {
            get { return current; }
        }
        #endregion Properties

        #region Constructor
        public TreeEnumerator(ITreeNodeManager<K, V> nodeManager,
            TreeNode<K, V> node,
            int fromIndex,
            TreeTraverseDirection direction)
        {
            this.nodeManager = nodeManager;
            this.currentNode = node;
            this.currentEntry = fromIndex;
            this.direction = direction;
        }
        #endregion Constructor

        #region Methods (public)
        public bool MoveNext()
        {
            if (isDoneIterating)
            { return false; }

            switch (this.direction)
            {
                case TreeTraverseDirection.Ascending:
                    return MoveForward();
                case TreeTraverseDirection.Descending:
                    return MoveBackward();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        bool MoveForward()
        {
            // For leaf node: move either right or up.
            // For parent node: always move right & down.
            if (currentNode.IsLeaf)
            {
                // Start by moving right.
                currentEntry++;

                while (true)
                {
                    // Return if current entry is valid,
                    // If can't move right then up.
                    // If can't move up, stop iterating.
                    if (currentEntry < currentNode.EntriesCount)
                    {
                        current = currentNode.GetEntry(currentEntry);
                        return true;
                    }
                    else if (currentNode.ParentId != 0)
                    {
                        currentEntry = currentNode.IndexInParent();
                        currentNode = nodeManager.Find(currentNode.ParentId);

                        if (currentEntry < 0 || currentNode == null)
                        { throw new Exception("Something gone wrong with BTree."); }
                    }
                    else
                    {
                        current = null;
                        isDoneIterating = true;
                        return false;
                    }
                }
            }
            else
            {
                currentEntry++;

                do
                {
                    currentNode = currentNode.GetChildNode(currentEntry);
                    currentEntry = 0;
                } while (!currentNode.IsLeaf);

                current = currentNode.GetEntry(currentEntry);
                return true;
            }
        }

        bool MoveBackward()
        {
            // For leaf node: move either right or up.
            // For parent node: always move left & down.
            if (currentNode.IsLeaf)
            {
                // Start by moving left.
                currentEntry--;

                while (true)
                {
                    // Return if current entry is valid.
                    // If can't move left then up.
                    // If can't move up, stop iterating.
                    if (currentEntry >= 0)
                    {
                        current = currentNode.GetEntry(currentEntry);
                        return true;
                    }
                    else if (currentNode.ParentId != 0)
                    {
                        currentEntry = currentNode.IndexInParent() - 1;
                        currentNode = nodeManager.Find(currentNode.ParentId);

                        if (currentNode == null)
                        { throw new Exception("Something gone wrong with BTree."); }
                    }
                    else
                    {
                        current = null;
                        isDoneIterating = true;
                        return false;
                    }
                }
            }
            else
            {
                currentEntry--;

                do
                {
                    currentNode = currentNode.GetChildNode(currentEntry);
                    currentEntry = currentNode.EntriesCount;

                    if (currentEntry < 0 || currentNode == null)
                    { throw new Exception("Something gone wrong with BTree."); }
                } while (!currentNode.IsLeaf);

                current = currentNode.GetEntry(currentEntry);
                return true;
            }
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        { }
        #endregion Methods (public)
    }
}