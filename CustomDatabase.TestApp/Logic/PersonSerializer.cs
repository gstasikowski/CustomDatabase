using CustomDatabase.Helpers;
using CustomDatabase.TestApp.Models;

namespace CustomDatabase.TestApp.Logic
{
    public class PersonSerializer
    {
        const int GuidIdLength = 16;
        const int FirstNameLength = 4;
        const int LastNameLength = 4;
        const int EmailLength = 4;
        const int PhoneNumberLength = 4;

        public byte[] Serialize(PersonModel person)
        {
            byte[] firstNameBytes = System.Text.Encoding.UTF8.GetBytes(person.FirstName);
            byte[] lastNameBytes = System.Text.Encoding.UTF8.GetBytes(person.LastName);
            byte[] emailBytes = System.Text.Encoding.UTF8.GetBytes(person.Email);
            byte[] phoneNumberBytes = System.Text.Encoding.UTF8.GetBytes(person.PhoneNumber);
            byte[] personData = new byte[
                GuidIdLength +
                FirstNameLength +
                firstNameBytes.Length +
                LastNameLength +
                lastNameBytes.Length +
                EmailLength +
                emailBytes.Length +
                PhoneNumberLength +
                phoneNumberBytes.Length
                ];

            int offset = 0;

            // ID
            Buffer.BlockCopy(
                src: person.Id.ToByteArray(),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: GuidIdLength
                );

            offset = 16;

            // First name
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)firstNameBytes.Length),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: FirstNameLength
                );

            offset += 4;

            Buffer.BlockCopy(
                src: firstNameBytes,
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: firstNameBytes.Length
                );

            offset += firstNameBytes.Length;

            // Last name
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)lastNameBytes.Length),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: LastNameLength
                );

            offset += 4;

            Buffer.BlockCopy(
                src: lastNameBytes,
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: lastNameBytes.Length
                );

            offset += lastNameBytes.Length;

            // Email
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)emailBytes.Length),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: EmailLength
                );

            offset += 4;

            Buffer.BlockCopy(
                src: emailBytes,
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: emailBytes.Length
                );

            offset += emailBytes.Length;

            // Phone number
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)phoneNumberBytes.Length),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: PhoneNumberLength
                );

            offset += 4;

            Buffer.BlockCopy(
                src: phoneNumberBytes,
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: phoneNumberBytes.Length
                );

            return personData;
        }

        public PersonModel Deserialize(byte[] data)
        {
            var personModel = new PersonModel();
            int offset = 0;

            // ID
            personModel.Id = BufferHelper.ReadBufferGuid(buffer: data, bufferOffset: offset);
            offset = GuidIdLength;

            // First name
            int firstNameLength = BufferHelper.ReadBufferInt32(buffer: data, bufferOffset: offset);
            offset += FirstNameLength;

            if (firstNameLength < 0 || firstNameLength > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + firstNameLength
                );
            }

            personModel.FirstName = System.Text.Encoding.UTF8.GetString(
                bytes: data,
                index: offset,
                count: firstNameLength
            );
            offset += firstNameLength;

            // Last name
            int lastNameLength = BufferHelper.ReadBufferInt32(buffer: data, bufferOffset: offset);
            offset += LastNameLength;

            if (lastNameLength < 0 || lastNameLength > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + lastNameLength
                );
            }

            personModel.LastName = System.Text.Encoding.UTF8.GetString(
                bytes: data,
                index: offset,
                count: lastNameLength);
            offset += lastNameLength;

            // Email
            int emailLength = BufferHelper.ReadBufferInt32(buffer: data, bufferOffset: offset);
            offset += EmailLength;

            if (emailLength < 0 || emailLength > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + emailLength
                );
            }

            personModel.Email = System.Text.Encoding.UTF8.GetString(
                bytes: data,
                index: offset,
                count: emailLength
            );
            offset += emailLength;

            // Phone number
            int phoneNumberLength = BufferHelper.ReadBufferInt32(buffer: data, bufferOffset: offset);
            offset += PhoneNumberLength;

            if (phoneNumberLength < 0 || phoneNumberLength > (16 * 1024))
            {
                throw new Exception(
                    CustomDatabase.CommonResources.GetErrorMessage("InvalidStringLength") + phoneNumberLength
                );
            }

            personModel.PhoneNumber = System.Text.Encoding.UTF8.GetString(
                bytes: data,
                index: offset,
                count: phoneNumberLength);

            return personModel;
        }
    }
}