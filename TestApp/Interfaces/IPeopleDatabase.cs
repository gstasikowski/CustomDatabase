using TestApp.Models;

namespace TestApp.Interfaces
{
    public interface IPeopleDatabase
    {
        void Insert(PersonModel person);
        void Delete(PersonModel person);
        void Update(PersonModel person);
        PersonModel Find(Guid id);
        IEnumerable<PersonModel> FindBy(string firstName, string lastName);
    }
}