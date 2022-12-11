using CustomDatabase.Interfaces;
using CustomDatabase.Helpers;

namespace CustomDatabase.Logic.Tree
{
    public sealed class TreeDiskNodeSerializer<K, V>
    {
        #region Variables
        private ISerializer<K> _keySerializer;
        private ISerializer<V> _valueSerializer;
        private ITreeNodeManager<K, V> _nodeManager;
        #endregion Variables

        #region Constructor
        public TreeDiskNodeSerializer(
            ITreeNodeManager<K, V> nodeManager,
            ISerializer<K> keySerializer,
            ISerializer<V> valueSerializer
        )
        {
            if (nodeManager == null)
            {
                throw new ArgumentNullException("nodeManager");
            }

            if (keySerializer == null)
            {
                throw new ArgumentNullException("keySerializer");
            }

            if (valueSerializer == null)
            {
                throw new ArgumentNullException("valueSerializer");
            }

            this._nodeManager = nodeManager;
            this._keySerializer = keySerializer;
            this._valueSerializer = valueSerializer;
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
            if (_keySerializer.IsFixedSize && _valueSerializer.IsFixedSize)
            {
                return FixedLengthDeserialize(assignId: assignId, buffer: record);
            }
            else if (_valueSerializer.IsFixedSize)
            {
                return VariableLengthDeserialize(assignId: assignId, buffer: record);
            }
            else
            {
                throw new NotSupportedException();
            }
        }

        /// <summary>
        /// Serialize node into byte array that will be written down by RecordStorage.
        /// </summary>
        /// <param name="node"></param>
        public byte[] Serialize(TreeNode<K, V> node)
        {
            if (_keySerializer.IsFixedSize && _valueSerializer.IsFixedSize)
            {
                return FixedLengthSerialize(node);
            }
            else if (_valueSerializer.IsFixedSize)
            {
                return VariableLengthSerialize(node);
            }
            else
            {
                throw new NotSupportedException();
            }
        }
        #endregion Methods (public)

        #region Methods (private)
        private byte[] FixedLengthSerialize(TreeNode<K, V> node)
        {
            int entrySize = this._keySerializer.Length + this._valueSerializer.Length;
            int size = 16 + (node.Entries.Length * entrySize) + (node.ChildrenIds.Length * 4);

            if (size >= (1024 * 64))
            {
                throw new Exception(CommonResources.GetErrorMessage("SerializedNodeTooLarge") + size);
            }

            byte[] buffer = new byte[size];

            // First 4 bytes of the buffer are parentID of this node.
            BufferHelper.WriteBuffer(value: node.ParentId, buffer: buffer, bufferOffset: 0);

            // Next 4 are number of entries.
            BufferHelper.WriteBuffer(value: (uint)node.EntriesCount, buffer: buffer, bufferOffset: 4);

            // Next 4 are number of children references.
            BufferHelper.WriteBuffer(value: (uint)node.ChildrenNodeCount, buffer: buffer, bufferOffset: 8);

            // Writing entries
            for (int index = 0; index < node.EntriesCount; index++)
            {
                var entry = node.GetEntry(index);

                Buffer.BlockCopy(
                    src: this._keySerializer.Serialize(entry.Item1),
                    srcOffset: 0,
                    dst: buffer,
                    dstOffset: 12 + (index * entrySize),
                    count: this._keySerializer.Length
                );
                Buffer.BlockCopy(
                    src: this._valueSerializer.Serialize(entry.Item2),
                    srcOffset: 0,
                    dst: buffer,
                    dstOffset: 12 + (index * entrySize) + this._keySerializer.Length,
                    count: this._valueSerializer.Length
                );
            }

            // Writing children refs
            uint[] childrenIds = node.ChildrenIds;

            for (int index = 0; index < node.ChildrenNodeCount; index++)
            { 
                BufferHelper.WriteBuffer(
                    value: childrenIds[index],
                    buffer: buffer,
                    bufferOffset: 12 + (entrySize * node.EntriesCount) + (index * 4)
                ); 
            }

            return buffer;
        }

        private TreeNode<K, V> FixedLengthDeserialize(uint assignId, byte[] buffer)
        {
            int entrySize = this._keySerializer.Length + this._valueSerializer.Length;

            // First 4 bytes of uint32 are parentID of this node.
            uint parentId = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 0);

            // Next 4 are number of entries.
            uint entriesCount = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 4);

            // Next 4 are number of children references.
            uint childrenCount = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 8);

            // Deserializing entries
            var entries = new Tuple<K, V>[entriesCount];

            for (int index = 0; index < entriesCount; index++)
            {
                var key = this._keySerializer.Deserialize(
                    buffer: buffer,
                    offset: 12 + (index * entrySize),
                    length: this._keySerializer.Length
                );
                var value = this._valueSerializer.Deserialize(
                    buffer: buffer,
                    offset: 12 + (index * entrySize) + this._keySerializer.Length,
                    length: this._valueSerializer.Length
                );

                entries[index] = new Tuple<K, V>(item1: key, item2: value);
            }

            // Reading child refs
            uint[] children = new uint[childrenCount];

            for (int index = 0; index < childrenCount; index++)
            {
                children[index] = BufferHelper.ReadBufferUInt32(
                    buffer: buffer,
                    bufferOffset: (int)(12 + (entrySize * entriesCount) + (index * 4))
                );
            }

            // Reconstructing the node
            return new TreeNode<K, V>(
                nodeManager: _nodeManager,
                id: assignId,
                parentId: parentId,
                entries: entries,
                childrenIds: children
            );
        }

        private byte[] VariableLengthSerialize(TreeNode<K, V> node)
        {
            using (var memorystream = new MemoryStream())
            {
                // First 4 bytes of the buffer are parentID of this node.
                memorystream.Write(
                    buffer: LittleEndianByteOrder.GetBytes((uint)node.ParentId),
                    offset: 0,
                    count: 4
                );

                // Next 4 are number of entries.
                memorystream.Write(
                    buffer: LittleEndianByteOrder.GetBytes((uint)node.EntriesCount),
                    offset: 0,
                    count: 4
                );

                // Next 4 are number of children references.
                memorystream.Write(
                    buffer: LittleEndianByteOrder.GetBytes((uint)node.ChildrenNodeCount),
                    offset: 0,
                    count: 4
                );

                // Writing entries
                for (int index = 0; index < node.EntriesCount; index++)
                {
                    var entry = node.GetEntry(index);
                    var key = this._keySerializer.Serialize(entry.Item1);
                    var value = this._valueSerializer.Serialize(entry.Item2);

                    memorystream.Write(
                        buffer: LittleEndianByteOrder.GetBytes((int)key.Length),
                        offset: 0,
                        count: 4
                    );
                    memorystream.Write(buffer: key, offset: 0, count: key.Length);
                    memorystream.Write(buffer: value, offset: 0, count: value.Length);
                }

                // Write children refs
                uint[] childrenIds = node.ChildrenIds;

                for (int index = 0; index < node.ChildrenNodeCount; index++)
                {
                    memorystream.Write(
                        buffer: LittleEndianByteOrder.GetBytes(childrenIds[index]),
                        offset: 0,
                        count: 4
                    );
                }

                return memorystream.ToArray();
            }
        }

        private TreeNode<K, V> VariableLengthDeserialize(uint assignId, byte[] buffer)
        {
            // First 4 bytes of the buffer are parentID of this node.
            uint parentId = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 0);

            // Next 4 are number of entries.
            uint entriesCount = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 4);

            // Next 4 are number of children references.
            uint childrenCount = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: 8);

            // Deserializing entries
            var entries = new Tuple<K, V>[entriesCount];
            int offset = 12;

            for (int index = 0; index < entriesCount; index++)
            {
                var keyLength = BufferHelper.ReadBufferUInt32(buffer: buffer, bufferOffset: offset);
                var key = this._keySerializer.Deserialize(
                    buffer: buffer,
                    offset: offset + 4,
                    length: (int)keyLength
                );
                var value = this._valueSerializer.Deserialize(
                    buffer: buffer,
                    offset: offset + 4 + (int)keyLength,
                    length: this._valueSerializer.Length
                );

                entries[index] = new Tuple<K, V>(item1: key, item2: value);
                offset += 4 + (int)keyLength + _valueSerializer.Length;
            }

            // Reading children refs
            uint[] children = new uint[childrenCount];

            for (int index = 0; index < childrenCount; index++)
            {
                children[index] = BufferHelper.ReadBufferUInt32(
                    buffer: buffer,
                    bufferOffset: offset + (index * 4)
                );
            }

            return new TreeNode<K, V>(
                nodeManager: _nodeManager,
                id: assignId,
                parentId: parentId,
                entries: entries,
                childrenIds: children
            );
        }
        #endregion Methods (private)
    }
}