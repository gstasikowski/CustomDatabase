namespace CustomDatabase.Exceptions
{
    public class TreeKeyExistsException : Exception
    {
        public TreeKeyExistsException(object key) : base(
            CommonResources.GetErrorMessage("TreeKeyExistsException") + key.ToString()
        )
        { }
    }
}