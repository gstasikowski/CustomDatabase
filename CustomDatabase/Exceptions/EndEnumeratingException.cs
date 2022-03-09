using System;

namespace CustomDatabase.Exceptions
{
    class EndEnumeratingException : Exception
    {
        public EndEnumeratingException(object key) : base("EndEnumeratingException: " + key.ToString())
        { }
    }
}
