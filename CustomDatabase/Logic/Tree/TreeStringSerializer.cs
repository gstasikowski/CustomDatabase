using CustomDatabase.Interfaces;
using System;

namespace CustomDatabase.Logic.Tree
{
    public class TreeStringSerializer : ISerializer<string>
    {
        #region Getters
        public bool IsFixedSize
        {
            get { return false; }
        }

        public int Length
        {
            get { throw new InvalidOperationException(); }
        }
        #endregion

        #region Methods (public)
        public string Deserialize(byte[] buffer, int offset, int length)
        {
            return System.Text.Encoding.UTF8.GetString(buffer, offset, length);
        }

        public byte[] Serialize(string value)
        {
            return System.Text.Encoding.UTF8.GetBytes(value);
        }
        #endregion
    }
}
