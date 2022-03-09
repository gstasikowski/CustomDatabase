using CustomDatabase.Helpers;
using System;
using TestApp.Models;

namespace TestApp.Logic
{
    public class CowSerializer
    {
        public byte[] Serialize(CowModel cow)
        {
            var breedBytes = System.Text.Encoding.UTF8.GetBytes(cow.Breed);
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(cow.Breed);
            var cowData = new byte[
                16 +                   // 16 bytes for Guid ID
                4 +                    // 4 bytes indicate the length of breed string
                breedBytes.Length +    // n bytes for breed string
                4 +                    // 4 bytes indicate the length of the name string
                nameBytes.Length +     // z bytes for name
                4 +                    // 4 bytes for age
                4 +                    // 4 bytes indicate length of DNA data
                cow.DnaData.Length     // y bytes of DNA data
                ];

            // ID
            Buffer.BlockCopy(
                src: cow.ID.ToByteArray(),
                srcOffset: 0,
                dst: cowData,
                dstOffset: 0,
                count: 16
                );

            // Breed
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)breedBytes.Length),
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16,
                count: 4
                );

            Buffer.BlockCopy(
                src: breedBytes,
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4,
                count: breedBytes.Length
                );

            // Name
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)nameBytes.Length),
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4 + breedBytes.Length,
                count: 4
                );

            Buffer.BlockCopy(
                src: nameBytes,
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4 + breedBytes.Length + 4,
                count: nameBytes.Length
                );

            // Age
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)cow.Age),
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4 + breedBytes.Length + 4 + nameBytes.Length,
                count: 4
                );

            // DNA
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes(cow.DnaData.Length),
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4 + breedBytes.Length + 4 + nameBytes.Length + 4,
                count: 4
                );

            Buffer.BlockCopy(
                src: cow.DnaData,
                srcOffset: 0,
                dst: cowData,
                dstOffset: 16 + 4 + breedBytes.Length + 4 + nameBytes.Length + 4 + 4,
                count: cow.DnaData.Length
                );

            return cowData;
        }

        public CowModel Deserialize(byte[] data)
        {
            var cowModel = new CowModel();

            // ID
            cowModel.ID = BufferHelper.ReadBufferGuid(data, 0);

            // Breed
            var breedLength = BufferHelper.ReadBufferInt32(data, 16);

            if (breedLength < 0 || breedLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + breedLength); }

            cowModel.Breed = System.Text.Encoding.UTF8.GetString(data, 16 + 4, breedLength);

            // Name
            var nameLength = BufferHelper.ReadBufferInt32(data, 16 + 4 + breedLength);

            if (nameLength < 0 || nameLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + nameLength); }

            cowModel.Name = System.Text.Encoding.UTF8.GetString(data, 16 + 4 + breedLength + 4, nameLength);

            // Age
            cowModel.Age = BufferHelper.ReadBufferInt32(data, 16 + 4 + breedLength + 4 + nameLength);

            // DNA data
            var dnaLength = BufferHelper.ReadBufferInt32(data, 16 + 4 + breedLength + 4 + nameLength + 4);

            if (dnaLength < 0 || dnaLength > (64 * 1024))
            { throw new Exception("Invalid DNA data length: " + dnaLength); }

            cowModel.DnaData = new byte[dnaLength];
            Buffer.BlockCopy(src: data, srcOffset: 16 + 4 + breedLength + 4 + nameLength + 4 + 4, dst: cowModel.DnaData, dstOffset: 0, count: cowModel.DnaData.Length);

            return cowModel;
        }
    }
}
