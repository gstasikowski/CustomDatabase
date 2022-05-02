using TestApp.Models;
using System;
using System.Collections.Generic;

namespace TestApp.Interfaces
{
    public interface IPeopleDatabase
    {
        void Insert(PersonModel person);
        void Delete(PersonModel person);
        void Update(PersonModel person);
        PersonModel Find(Guid ID);
        IEnumerable<PersonModel> FindBy(string firstName, string lastName);
    }
}
