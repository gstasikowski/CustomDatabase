using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System.Collections;

namespace CustomDatabase.Logic
{
    public class TreeEnumerator<K, V> : IEnumerator<Tuple<K, V>>
    {
        #region Variables
        private readonly ITreeNodeManager<K, V> _nodeManager;
        private readonly TreeTraverseDirection _direction;

        private bool _isDoneIterating = false;
        private int _currentEntry = 0;
        private TreeNode<K, V> _currentNode;
        private Tuple<K, V> _current;
        #endregion Variables

        #region Properties
        public TreeNode<K, V> CurrentNode
        {
            get { return _currentNode; }
        }

        public int CurrentEntry
        {
            get { return _currentEntry; }
        }

        object IEnumerator.Current
        {
            get { return (object)Current; }
        }

        public Tuple<K, V> Current
        {
            get { return _current; }
        }
        #endregion Properties

        #region Constructor
        public TreeEnumerator(
            ITreeNodeManager<K, V> nodeManager,
            TreeNode<K, V> node,
            int fromIndex,
            TreeTraverseDirection direction
        )
        {
            this._nodeManager = nodeManager;
            this._currentNode = node;
            this._currentEntry = fromIndex;
            this._direction = direction;
        }
        #endregion Constructor

        #region Methods (public)
        public bool MoveNext()
        {
            if (_isDoneIterating)
            {
                return false;
            }

            switch (this._direction)
            {
                case TreeTraverseDirection.Ascending:
                    return MoveForward();

                case TreeTraverseDirection.Descending:
                    return MoveBackward();

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        { }
        #endregion Methods (public)

        #region Methods (private)
        private bool MoveForward()
        {
            // For leaf node: move either right or up.
            // For parent node: always move right & down.
            if (_currentNode.IsLeaf)
            {
                // Start by moving right.
                _currentEntry++;

                while (true)
                {
                    // Return if current entry is valid,
                    // If can't move right then up.
                    // If can't move up, stop iterating.
                    if (_currentEntry < _currentNode.EntriesCount)
                    {
                        _current = _currentNode.GetEntry(_currentEntry);
                        return true;
                    }
                    else if (_currentNode.ParentId != 0)
                    {
                        _currentEntry = _currentNode.IndexInParent();
                        _currentNode = _nodeManager.Find(_currentNode.ParentId);

                        if (_currentEntry < 0 || _currentNode == null)
                        {
                            throw new Exception(CommonResources.GetErrorMessage("BTreeIssue"));
                        }
                    }
                    else
                    {
                        _current = null;
                        _isDoneIterating = true;
                        return false;
                    }
                }
            }
            else
            {
                _currentEntry++;

                do
                {
                    _currentNode = _currentNode.GetChildNode(_currentEntry);
                    _currentEntry = 0;
                } while (!_currentNode.IsLeaf);

                _current = _currentNode.GetEntry(_currentEntry);
                return true;
            }
        }

        private bool MoveBackward()
        {
            // For leaf node: move either right or up.
            // For parent node: always move left & down.
            if (_currentNode.IsLeaf)
            {
                // Start by moving left.
                _currentEntry--;

                while (true)
                {
                    // Return if current entry is valid.
                    // If can't move left then up.
                    // If can't move up, stop iterating.
                    if (_currentEntry >= 0)
                    {
                        _current = _currentNode.GetEntry(_currentEntry);
                        return true;
                    }
                    else if (_currentNode.ParentId != 0)
                    {
                        _currentEntry = _currentNode.IndexInParent() - 1;
                        _currentNode = _nodeManager.Find(_currentNode.ParentId);

                        if (_currentNode == null)
                        {
                            throw new Exception(CommonResources.GetErrorMessage("BTreeIssue"));
                        }
                    }
                    else
                    {
                        _current = null;
                        _isDoneIterating = true;
                        return false;
                    }
                }
            }
            else
            {
                _currentEntry--;

                do
                {
                    _currentNode = _currentNode.GetChildNode(_currentEntry);
                    _currentEntry = _currentNode.EntriesCount;

                    if (_currentEntry < 0 || _currentNode == null)
                    {
                        throw new Exception(CommonResources.GetErrorMessage("BTreeIssue"));
                    }
                } while (!_currentNode.IsLeaf);

                _current = _currentNode.GetEntry(_currentEntry);
                return true;
            }
        }
        #endregion Methods (private)
    }
}