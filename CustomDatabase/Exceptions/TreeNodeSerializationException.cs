namespace CustomDatabase.Exceptions
{
    public class TreeNodeSerializationException : Exception
    {
        public TreeNodeSerializationException(Exception innerException) : base(
            message: CommonResources.GetErrorMessage("TreeNodeSerializationException"),
            innerException: innerException
        )
        { }
    }
}