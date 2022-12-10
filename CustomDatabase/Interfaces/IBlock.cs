namespace CustomDatabase.Interfaces
{
    public interface IBlock : IDisposable
    {
        // <summary>
        // ID of the block, must be unique.
        // </summary>
        uint Id { get; }

        // <summary>
        // Block may contain one or more header metadata,
        // each header identified by a number and 8 bytes value.
        // </summary>
        long GetHeader(int field);

        // <summary>
        // Change the value of specific header.
        // Data must not be written to disk until block is disposed.
        // </summary>
        void SetHeader(int field, long value);

        // <summary>
        // Read content of this block (src) into given buffer (dst).
        // </summary>
        void Read(byte[] destination, int destinationOffset, int sourceOffset, int count);

        // <summary>
        // Write content of given buffer (src) into this (dst).
        // </summary>
        void Write(byte[] source, int sourceOffset, int destinationOffset, int count);
    }
}