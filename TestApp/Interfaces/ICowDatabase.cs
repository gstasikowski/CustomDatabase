using TestApp.Models;
using System;
using System.Collections.Generic;

namespace TestApp.Interfaces
{
    public interface ICowDatabase
    {
        void Insert(PersonModel cow);
        void Delete(PersonModel cow);
        void Update(PersonModel cow);
        PersonModel Find(Guid ID);
        IEnumerable<PersonModel> FindBy(string breed, int age);
    }
}
