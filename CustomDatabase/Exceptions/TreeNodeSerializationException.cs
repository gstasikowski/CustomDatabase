using System;

namespace CustomDatabase.Exceptions
{
    public class TreeNodeSerializationException : Exception
    {
        public TreeNodeSerializationException(Exception innerException)
            : base("Failed to (de)serialize heat map node.", innerException)
        { }
    }
}
