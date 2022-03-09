using TestApp.Models;
using System;
using System.Collections.Generic;

namespace TestApp.Interfaces
{
    public interface ICowDatabase
    {
        void Insert(CowModel cow);
        void Delete(CowModel cow);
        void Update(CowModel cow);
        CowModel Find(Guid ID);
        IEnumerable<CowModel> FindBy(string breed, int age);
    }
}
