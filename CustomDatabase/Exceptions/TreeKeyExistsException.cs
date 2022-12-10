namespace CustomDatabase.Exceptions
{
    public class TreeKeyExistsException : Exception
    {
        public TreeKeyExistsException(object key) : base ("Duplicate key: " + key.ToString())
        { }
    }
}