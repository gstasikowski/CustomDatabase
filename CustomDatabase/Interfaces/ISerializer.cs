namespace CustomDatabase.Interfaces
{
    public interface ISerializer<K>
    {
        K Deserialize(byte[] buffer, int offset, int length);

        byte[] Serialize(K value);

        bool IsFixedSize
        { get; }

        int Length
        { get; }
    }
}
