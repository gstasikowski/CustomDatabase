using System;

namespace CustomDatabase.Interfaces
{
    public interface IRecordStorage
    {
        // <summary>
        // Effectively update a record.
        // </summary>
        void Update(uint recordID, byte[] data);

        // <summary>
        // Get record's data.
        // </summary>
        byte[] Find(uint recordID);

        // <summary>
        // Create new empty record.
        // </summary>
        uint Create(byte[] data);

        // <summary>
        // Similar to above but with addition of dataGenerator
        // which generates data after a record is allocated.
        // </summary>
        uint Create(Func<uint, byte[]> dataGenerator);

        // <summary>
        // Delete a record by its ID.
        // </summary>
        void Delete(uint recordID);
    }
}
