namespace CustomDatabase.Exceptions
{
    class EndEnumeratingException : Exception
    {
        public EndEnumeratingException(object key) : base(
            CommonResources.GetErrorMessage("EndEnumeratingException") + key.ToString()
        )
        { }
    }
}