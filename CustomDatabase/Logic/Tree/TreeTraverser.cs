using CustomDatabase.Helpers;
using CustomDatabase.Interfaces;
using System.Collections;

namespace CustomDatabase.Logic.Tree
{
    public class TreeTraverser<K, V> : IEnumerable<Tuple<K, V>>
    {
        #region Variables
        private readonly TreeNode<K, V> fromNode;
        private readonly int fromIndex;
        private readonly TreeTraverseDirection direction;
        private readonly ITreeNodeManager<K, V> nodeManager;
        #endregion Variables

        #region Constructor
        public TreeTraverser(ITreeNodeManager<K, V> nodeManager,
            TreeNode<K, V> fromNode,
            int fromIndex,
            TreeTraverseDirection direction)
        {
            if (fromNode == null)
            { throw new ArgumentNullException("fromNode"); }

            this.nodeManager = nodeManager;
            this.fromNode = fromNode;
            this.fromIndex = fromIndex;
            this.direction = direction;
        }
        #endregion Constructor

        #region Properties
        public IEnumerator<Tuple<K, V>> GetEnumerator()
        {
            return new TreeEnumerator<K, V>(nodeManager, fromNode, fromIndex, direction);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<Tuple<K, V>>)this).GetEnumerator();
        }
        #endregion Properties
    }
}