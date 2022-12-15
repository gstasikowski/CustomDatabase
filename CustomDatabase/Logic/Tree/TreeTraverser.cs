using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System.Collections;

namespace CustomDatabase.Logic
{
    public class TreeTraverser<K, V> : IEnumerable<Tuple<K, V>>
    {
        #region Variables
        private readonly TreeNode<K, V> _fromNode;
        private readonly int _fromIndex;
        private readonly TreeTraverseDirection _direction;
        private readonly ITreeNodeManager<K, V> _nodeManager;
        #endregion Variables

        #region Constructor
        public TreeTraverser(
            ITreeNodeManager<K, V> nodeManager,
            TreeNode<K, V> fromNode,
            int fromIndex,
            TreeTraverseDirection direction
        )
        {
            if (fromNode == null)
            {
                throw new ArgumentNullException("fromNode");
            }

            this._nodeManager = nodeManager;
            this._fromNode = fromNode;
            this._fromIndex = fromIndex;
            this._direction = direction;
        }
        #endregion Constructor

        #region Properties
        public IEnumerator<Tuple<K, V>> GetEnumerator()
        {
            return new TreeEnumerator<K, V>(
                nodeManager: _nodeManager,
                node: _fromNode,
                fromIndex: _fromIndex,
                direction: _direction
            );
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<Tuple<K, V>>)this).GetEnumerator();
        }
        #endregion Properties
    }
}