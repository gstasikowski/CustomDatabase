using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CustomDatabase.Exceptions
{
    public class TreeNodeSerializationException : Exception
    {
        public TreeNodeSerializationException(Exception innerException)
            : base("Failed to (de)serialize heat map node.", innerException)
        { }
    }
}
