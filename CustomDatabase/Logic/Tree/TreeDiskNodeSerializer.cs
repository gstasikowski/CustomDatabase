using CustomDatabase.Interfaces;
using CustomDatabase.Helpers;

namespace CustomDatabase.Logic.Tree
{
    public sealed class TreeDiskNodeSerializer<K, V>
    {
        #region Variables
        private ISerializer<K> keySerializer;
        private ISerializer<V> valueSerializer;
        private ITreeNodeManager<K, V> nodeManager;
        #endregion Variables

        #region Constructor
        public TreeDiskNodeSerializer(ITreeNodeManager<K, V> nodeManager,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer)
        {
            if (nodeManager == null)
            { throw new ArgumentNullException("nodeManager"); }

            if (keySerializer == null)
            { throw new ArgumentNullException("keySerializer"); }

            if (valueSerializer == null)
            { throw new ArgumentNullException("valueSerializer"); }

            this.nodeManager = nodeManager;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Deserialize node from data read by RecordStorage.
        /// </summary>
        /// <param name="assignId"></param>
        /// <param name="record"></param>
        public TreeNode<K, V> Deserialize(uint assignId, byte[] record)
        {
            if (keySerializer.IsFixedSize && valueSerializer.IsFixedSize)
            { return FixedLengthDeserialize(assignId, record); }
            else if (valueSerializer.IsFixedSize)
            { return VariableLengthDeserialize(assignId, record); }
            else
            { throw new NotSupportedException(); }
        }

        /// <summary>
        /// Serialize node into byte array that will be written down by RecordStorage.
        /// </summary>
        /// <param name="node"></param>
        public byte[] Serialize(TreeNode<K, V> node)
        {
            if (keySerializer.IsFixedSize && valueSerializer.IsFixedSize)
            { return FixedLengthSerialize(node); }
            else if (valueSerializer.IsFixedSize)
            { return VariableLengthSerialize(node); }
            else
            { throw new NotSupportedException(); }
        }
        #endregion Methods (public)

        #region Methods (private)
        byte[] FixedLengthSerialize(TreeNode<K, V> node)
        {
            int entrySize = this.keySerializer.Length + this.valueSerializer.Length;
            int size = 16 + (node.Entries.Length * entrySize) + (node.ChildrenIds.Length * 4);

            if (size >= (1024 * 64))
            { throw new Exception("Serialized node size is too large: " + size); }

            byte[] buffer = new byte[size];

            // First 4 bytes of the buffer are parentID of this node.
            BufferHelper.WriteBuffer(node.ParentId, buffer, 0);

            // Next 4 are number of entries.
            BufferHelper.WriteBuffer((uint)node.EntriesCount, buffer, 4);

            // Next 4 are number of children references.
            BufferHelper.WriteBuffer((uint)node.ChildrenNodeCount, buffer, 8);

            // Writing entries
            for (int i = 0; i < node.EntriesCount; i++)
            {
                var entry = node.GetEntry(i);

                Buffer.BlockCopy(this.keySerializer.Serialize(entry.Item1), 0, buffer, 12 + (i * entrySize), this.keySerializer.Length);
                Buffer.BlockCopy(this.valueSerializer.Serialize(entry.Item2), 0, buffer, 12 + (i * entrySize) + this.keySerializer.Length, this.valueSerializer.Length);
            }

            // Writing children refs
            uint[] childrenIds = node.ChildrenIds;

            for (int i = 0; i < node.ChildrenNodeCount; i++)
            { 
                BufferHelper.WriteBuffer(childrenIds[i], buffer, 12 + (entrySize * node.EntriesCount) + (i * 4)); 
            }

            return buffer;
        }

        TreeNode<K, V> FixedLengthDeserialize(uint assignId, byte[] buffer)
        {
            int entrySize = this.keySerializer.Length + this.valueSerializer.Length;

            // First 4 bytes of uint32 are parentID of this node.
            uint parentId = BufferHelper.ReadBufferUInt32(buffer, 0);

            // Next 4 are number of entries.
            uint entriesCount = BufferHelper.ReadBufferUInt32(buffer, 4);

            // Next 4 are number of children references.
            uint childrenCount = BufferHelper.ReadBufferUInt32(buffer, 8);

            // Deserializing entries
            var entries = new Tuple<K, V>[entriesCount];

            for (int i = 0; i < entriesCount; i++)
            {
                var key = this.keySerializer.Deserialize(buffer, 12 + (i * entrySize), this.keySerializer.Length);
                var value = this.valueSerializer.Deserialize(buffer, 12 + (i * entrySize) + this.keySerializer.Length, this.valueSerializer.Length);

                entries[i] = new Tuple<K, V>(key, value);
            }

            // Reading child refs
            uint[] children = new uint[childrenCount];

            for (int i = 0; i < childrenCount; i++)
            {
                children[i] = BufferHelper.ReadBufferUInt32(buffer, (int)(12 + (entrySize * entriesCount) + (i * 4)));
            }

            // Reconstructing the node
            return new TreeNode<K, V>(nodeManager, assignId, parentId, entries, children);
        }

        byte[] VariableLengthSerialize(TreeNode<K, V> node)
        {
            using (var memorystream = new MemoryStream())
            {
                // First 4 bytes of the buffer are parentID of this node.
                memorystream.Write(LittleEndianByteOrder.GetBytes((uint)node.ParentId), 0, 4);

                // Next 4 are number of entries.
                memorystream.Write(LittleEndianByteOrder.GetBytes((uint)node.EntriesCount), 0, 4);

                // Next 4 are number of children references.
                memorystream.Write(LittleEndianByteOrder.GetBytes((uint)node.ChildrenNodeCount), 0, 4);

                // Writing entries
                for (int i = 0; i < node.EntriesCount; i++)
                {
                    var entry = node.GetEntry(i);
                    var key = this.keySerializer.Serialize(entry.Item1);
                    var value = this.valueSerializer.Serialize(entry.Item2);

                    memorystream.Write(LittleEndianByteOrder.GetBytes((int)key.Length), 0, 4);
                    memorystream.Write(key, 0, key.Length);
                    memorystream.Write(value, 0, value.Length);
                }

                // Write children refs
                uint[] childrenIds = node.ChildrenIds;

                for (int i = 0; i < node.ChildrenNodeCount; i++)
                {
                    memorystream.Write(LittleEndianByteOrder.GetBytes(childrenIds[i]), 0, 4);
                }

                return memorystream.ToArray();
            }
        }

        TreeNode<K, V> VariableLengthDeserialize(uint assignId, byte[] buffer)
        {
            // First 4 bytes of the buffer are parentID of this node.
            uint parentId = BufferHelper.ReadBufferUInt32(buffer, 0);

            // Next 4 are number of entries.
            uint entriesCount = BufferHelper.ReadBufferUInt32(buffer, 4);

            // Next 4 are number of children references.
            uint childrenCount = BufferHelper.ReadBufferUInt32(buffer, 8);

            // Deserializing entries
            var entries = new Tuple<K, V>[entriesCount];
            int offset = 12;

            for (int i = 0; i < entriesCount; i++)
            {
                var keyLength = BufferHelper.ReadBufferUInt32(buffer, offset);
                var key = this.keySerializer.Deserialize(buffer, offset + 4, (int)keyLength);
                var value = this.valueSerializer.Deserialize(buffer, offset + 4 + (int)keyLength, this.valueSerializer.Length);

                entries[i] = new Tuple<K, V>(key, value);
                offset += 4 + (int)keyLength + valueSerializer.Length;
            }

            // Reading children refs
            uint[] children = new uint[childrenCount];

            for (int i = 0; i < childrenCount; i++)
            {
                children[i] = BufferHelper.ReadBufferUInt32(buffer, offset + (i * 4));
            }

            return new TreeNode<K, V>(nodeManager, assignId, parentId, entries, children);
        }
        #endregion Methods (private)
    }
}