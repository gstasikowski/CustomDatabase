using CustomDatabase.Interfaces;

namespace CustomDatabase.Logic.Tree
{
    public class TreeStringSerializer : ISerializer<string>
    {
        #region Properties
        public bool IsFixedSize
        {
            get { return false; }
        }

        public int Length
        {
            get { throw new InvalidOperationException(); }
        }
        #endregion Properties

        #region Methods (public)
        public string Deserialize(byte[] buffer, int offset, int length)
        {
            return System.Text.Encoding.UTF8.GetString(bytes: buffer, index: offset, count: length);
        }

        public byte[] Serialize(string value)
        {
            return System.Text.Encoding.UTF8.GetBytes(value);
        }
        #endregion Methods (public)
    }
}