using TestApp.Logic;
using TestApp.Models;

namespace TestApp
{
    class Program
    {
        public static void Main(string[] args)
        {
			// It's a test app so put the db file in a temp folder.
            string dbFile = Path.Combine(AppContext.BaseDirectory, "people.data");

			ShowMenu(dbFile);
		}

		private static void ShowMenu(string dbFile)
        {
			Console.Clear();
			Console.WriteLine("What do you want to do?\n1 - Quick test\n2 - Show all entries\n3 - Add new person\n4 - Find person by ID\n5 - Delete database files\n6 - Exit\n");
			string option = Console.ReadLine();

			switch (option)
			{
				case "1":
					QuickTest(dbFile);
					break;

				case "2":
					ShowAllEntries(dbFile);
					break;

				case "3":
					AddPerson(dbFile);
					break;

				case "4":
					Console.Write("Input ID to search for: ");
					FindEntryById(dbFile, Console.ReadLine());
					break;

				case "5":
					ClearDBFiles(dbFile);
					Console.WriteLine("\nPress key to continue...");
					Console.ReadKey();
					break;

				case "6":
					System.Environment.Exit(1);
					break;

				default:
					break;
			}

			ShowMenu(dbFile);
		}

		private static void QuickTest(string dbFile)
		{
			ClearDBFiles(dbFile);

			// Init the db based on a file on temp directory
			using (var db = new PeopleDatabase(dbFile))
			{
				// Insert some people into our database..
				db.Insert(new PersonModel
				{
					Id = Guid.Parse("8872d8ba-e470-440d-aa9b-071822e8053f"),
					FirstName = "Emilie",
					LastName = "Dundee",
					Email = "ed@email.com",
					PhoneNumber = "555-421-555"
				});
				Console.WriteLine("Inserted 1 person");

				db.Insert(new PersonModel
				{
					Id = Guid.Parse("59ee9033-4ec5-40e0-91a7-6c9ecb6e0465"),
					FirstName = "Randy",
					LastName = "Lopez",
					Email = "big-rand@email.com",
					PhoneNumber = "555-999-735"
				});
				Console.WriteLine("Inserted 2 people");
			}

			FindEntryById(dbFile, "8872d8ba-e470-440d-aa9b-071822e8053f");
			ShowAllEntries(dbFile);
		}

		private static void FindEntryById(string dbFile, string id)
		{
			// Reconstruct the database, to demonstrate that data is persistent
			using (var db = new PeopleDatabase(dbFile))
			{
				// Find a person by ID, 
				// This uses the primary index so the query is an ad-hoc query.
				var person = db.Find(Guid.Parse(id));
				string personInfo = string.Format("\n| ID: {0}\n| Name: {1} {2}\n| Email: {3}\n| Phone number: {4}", person.Id, person.FirstName, person.LastName, person.Email, person.PhoneNumber);
				Console.WriteLine(personInfo);
			}

			Console.WriteLine("\nPress key to continue...");
			Console.ReadKey();
		}

		private static void ShowAllEntries(string dbFile)
        {
			// Reconstruct the database, to demonstrate that data is persistent
			using (var db = new PeopleDatabase(dbFile))
			{
				// Display all entries
				Console.WriteLine("\nPeople found:\n-------------");
				foreach (var row in db.GetAll())
				{
					string personInfo = string.Format("\n| ID: {0}\n| Name: {1} {2}\n| Email: {3}\n| Phone number: {4}", row.Id, row.FirstName, row.LastName, row.Email, row.PhoneNumber);
					Console.WriteLine(personInfo);
				}

				Console.WriteLine("\nPress key to continue...");
				Console.ReadKey();
			}
		}

		private static void AddPerson(string dbFile)
		{
			var person = new PersonModel();

			person.Id = Guid.NewGuid();

			Console.Write("Enter first name: ");
			person.FirstName = Console.ReadLine();

			Console.Write("Enter last name: ");
			person.LastName = Console.ReadLine();

			Console.Write("Enter email name: ");
			person.Email = Console.ReadLine();

			Console.Write("Enter phone number name: ");
			person.PhoneNumber = Console.ReadLine();

			using (var db = new PeopleDatabase(dbFile))
			{
				db.Insert(person);
				Console.WriteLine("\nAdded new person");
			}
		}

		private static void ClearDBFiles(string dbFile)
        {
			if (File.Exists(dbFile))
			{
				File.Delete(dbFile);
				Console.WriteLine("Deleted main database file");
			}

			if (File.Exists(dbFile + ".pidx"))
			{
				File.Delete(dbFile + ".pidx");
				Console.WriteLine("Deleted primary index file");
			}

			if (File.Exists(dbFile + ".sidx"))
			{
				File.Delete(dbFile + ".sidx");
				Console.WriteLine("Deleted secondary index file");
			}
		}
    }
}