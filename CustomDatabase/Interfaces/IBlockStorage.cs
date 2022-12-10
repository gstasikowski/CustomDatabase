namespace CustomDatabase.Interfaces
{
    public interface IBlockStorage
    {
        // <summary>
        // Number of bytes of custom data per block that this storage can handle.
        // </summary>
        int BlockContentSize { get; }

        // <summary>
        // Total number of bytes in header.
        // </summary>
        int BlockHeaderSize { get; }

        // <summary>
        // Total block size, equal to content size + header size, should be multiple of 128B.
        // </summary>
        int BlockSize { get; }

        // <summary>
        // Allocate new block, extend the length of underlying storage.
        // </summary>
        IBlock CreateNew();

        // <summary>
        // Find a block by its ID.
        // </summary>
        IBlock Find(uint blockId);
    }
}