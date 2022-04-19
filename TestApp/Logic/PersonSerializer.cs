using CustomDatabase.Helpers;
using System;
using TestApp.Models;

namespace TestApp.Logic
{
    public class PersonSerializer
    {
        public byte[] Serialize(PersonModel person)
        {
            var firstNameBytes = System.Text.Encoding.UTF8.GetBytes(person.FirstName);
            var lastNameBytes = System.Text.Encoding.UTF8.GetBytes(person.LastName);
            var emailBytes = System.Text.Encoding.UTF8.GetBytes(person.Email);
            var phoneNumberBytes = System.Text.Encoding.UTF8.GetBytes(person.PhoneNumber);
            var personData = new byte[
                16 +                    // 16 bytes for Guid ID
                4 +                     // 4 bytes indicate the length of first name string
                firstNameBytes.Length + // n bytes for first name string
                4 +                     // 4 bytes indicate the length of the last name string
                lastNameBytes.Length +  // z bytes for last name
                4 +                     // 4 bytes indicate length of email string
                emailBytes.Length +     // y bytes of email string
                4 +                     // 4 bytes indicate length of phone number string
                phoneNumberBytes.Length // x bytes of phone number string
                ];

            int offset = 0;

            // ID
            Buffer.BlockCopy(
                src: person.ID.ToByteArray(),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: 16
                );

            offset = 16;

            // First name
            Buffer.BlockCopy(
                src: LittleEndianByteOrder.GetBytes((int)firstNameBytes.Length),
                srcOffset: 0,
                dst: personData,
                dstOffset: offset,
                count: 4
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
                count: 4
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
                count: 4
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
                count: 4
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
            personModel.ID = BufferHelper.ReadBufferGuid(data, offset);
            offset = 16;

            // First name
            var firstNameLength = BufferHelper.ReadBufferInt32(data, offset);
            offset += 4;

            if (firstNameLength < 0 || firstNameLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + firstNameLength); }

            personModel.FirstName = System.Text.Encoding.UTF8.GetString(data, offset, firstNameLength);
            offset += firstNameLength;

            // Last name
            var lastNameLength = BufferHelper.ReadBufferInt32(data, offset);
            offset += 4;

            if (lastNameLength < 0 || lastNameLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + lastNameLength); }

            personModel.LastName = System.Text.Encoding.UTF8.GetString(data, offset, lastNameLength);
            offset += lastNameLength;

            // Email
            var emailLength = BufferHelper.ReadBufferInt32(data, offset);
            offset += 4;

            if (emailLength < 0 || emailLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + emailLength); }

            personModel.Email = System.Text.Encoding.UTF8.GetString(data, offset, emailLength);
            offset += emailLength;

            // Phone number
            var phoneNumberLength = BufferHelper.ReadBufferInt32(data, offset);
            offset += 4;

            if (phoneNumberLength < 0 || phoneNumberLength > (16 * 1024))
            { throw new Exception("Invalid string length: " + phoneNumberLength); }

            personModel.PhoneNumber = System.Text.Encoding.UTF8.GetString(data, offset, phoneNumberLength);

            return personModel;
        }
    }
}
