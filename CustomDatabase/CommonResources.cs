namespace CustomDatabase
{
    public static class CommonResources
    {
        private static  Dictionary<string, string> _errorMessages = new Dictionary<string, string>()
        {
            { "BlockHeaderSizeTooBig", "blockHeaderSize can't be >= to blockSize" },
            { "BlockHeaderSizeTooSmall", "Record storage needs at least 48 header bytes." },
            { "BlockNotFound", "Block not found: " },
            { "BlockNotFoundById", "Block not found by ID: " },
            { "BTreeIssue", "Something gone wrong with BTree." },
            { "DestinationOutOfBounds", "Requested count is outside dest bounds. Count: " },
            { "EmptyBlock", "Trying to dequeue UInt32 from an empty block." },
            { "EndEnumeratingException", "EndEnumeratingException" },
            { "FailedToAllocateNewBlock", "Failed to allocate new block." },
            { "FailedToCallDataGenerator", "dataGenerator never called by nodeStorage." },
            { "FailedToCreateBlock", "Failed to create new block." },
            { "FailedToFindBlocks0", "Failed to find blocks of record 0." },
            { "FailedToFindIndexOfNodeInParent", "Failed to find index of node { Id } in its parent." },
            { "FailedToFindParentNode", "IndexInParent failed to find parent node of " },
            { "IncorrectBlockContentLength", "Block content length not %4: " },
            { "InvalidField", "Invalid field: " },
            { "InvalidLength", "Invalid length: " },
            { "InvalidStringLength", "Invalid string length: " },
            { "NodeNotFoundById", "Node not found by ID: " },
            { "NonUniqueTreeInvocation", "This method should be called only from non unique tree." },
            { "SerializedNodeTooLarge", "Serialized node size is too large: " },
            { "SourceOutOfBounds", "Requested count is outside of src bounds. Count: " },
            { "TreeKeyExistsException", "Duplicate key: " },
            { "TreeNodeSerializationException", "Failed to (de)serialize heat map node." },
            { "UnexpectedBlockContentLength", "Unexpected block content length: " },
            { "UnexpectedRecordLength", "Unexpected record length: " },
            { "UnexpectedStreamLength", "Unexpected length of the stream: " },
            { "UniqueTreeInvocation", "This method should be called only from unique tree." },
            { "WrongFirstSectorLength", "First sector length must be: " }
        };

        public static string GetErrorMessage(string id)
        {
            return (_errorMessages[id] != null) ? _errorMessages[id] : "Unknown error";
        }
    }
}