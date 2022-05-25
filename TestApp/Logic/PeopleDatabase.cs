using CustomDatabase.Logic;
using CustomDatabase.Logic.Tree;
using System;
using System.Collections.Generic;
using System.IO;
using TestApp.Models;

namespace TestApp.Logic
{
    class PeopleDatabase : IDisposable
    {
        #region Variables
        readonly Stream mainDatabaseFile;
        readonly Stream primaryIndexFile;
        readonly Stream secondaryIndexFile;
        readonly Tree<Guid, uint> primaryIndex;
        readonly Tree<Tuple<string, string>, uint> secondaryIndex;
        readonly RecordStorage peopleRecords;
        readonly PersonSerializer personSerializer = new PersonSerializer();
        #endregion Variables

        #region Constructor
        public PeopleDatabase(string pathToDBFile)
        {
            if (pathToDBFile == null)
            { throw new ArgumentNullException("pathToDBFile"); }

            // Open the stream and (create) database files.
            this.mainDatabaseFile = new FileStream(pathToDBFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);
            this.primaryIndexFile = new FileStream(pathToDBFile + ".pidx", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);
            this.secondaryIndexFile = new FileStream(pathToDBFile + ".sidx", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096);

            // Create a RecordStorage for main cow data
            this.peopleRecords = new RecordStorage(new BlockStorage(this.mainDatabaseFile, 4096, 48));

            // Create the indexes
            this.primaryIndex = new Tree<Guid, uint>(
                new TreeDiskNodeManager<Guid, uint>(
                    new GuidSerializer(),
                    new TreeUIntSerializer(),
                    new RecordStorage(new BlockStorage(this.primaryIndexFile, 4096))
                    ),
                false
            );

            this.secondaryIndex = new Tree<Tuple<string, string>, uint>(
				new TreeDiskNodeManager<Tuple<string, string>, uint>(
					new StringSerializer(), 
					new TreeUIntSerializer(), 
					new RecordStorage(new BlockStorage(this.secondaryIndexFile, 4096))
                    ),
                true
            );
        }
        #endregion Constructor

        #region Methods (public)
        /// <summary>
        /// Update entry.
        /// </summary>
        public void Update(PersonModel person)
        {
            if (disposed)
            { throw new ObjectDisposedException("PeopleDatabase"); }

            throw new NotImplementedException();
        }

        /// <summary>
        /// Insert new entry into database.
        /// </summary>
        public void Insert(PersonModel person)
        {
            if (disposed)
            { throw new ObjectDisposedException("PeopleDatabase"); }

            uint recordID = this.peopleRecords.Create(this.personSerializer.Serialize(person));

            this.primaryIndex.Insert(person.ID, recordID);
            this.secondaryIndex.Insert(new Tuple<string, string>(person.FirstName, person.LastName), recordID);
        }

        /// <summary>
        /// Find an entry by ID.
        /// </summary>
        public PersonModel Find(Guid ID)
        {
            if (disposed)
            { throw new ObjectDisposedException("PeopleDatabase"); }

            var entry = this.primaryIndex.Get(ID);

            if (entry == null)
            { return null; }

            return this.personSerializer.Deserialize(this.peopleRecords.Find(entry.Item2));
        }

        /// <summary>
        /// Find all entries within parameteres.
        /// </summary>
        public IEnumerable<PersonModel> FindBy(string firstName, string lastName)
        {
            var comparer = Comparer<Tuple<string, string>>.Default;
            var searchKey = new Tuple<string, string>(firstName, lastName);

            foreach (var entry in this.secondaryIndex.LargerThanOrEqualTo(searchKey))
            {
                // Stop upon reaching key larger than provided
                if (comparer.Compare(entry.Item1, searchKey) > 0)
                { break; }

                yield return this.personSerializer.Deserialize(this.peopleRecords.Find(entry.Item2));
            }
        }

        /// <summary>
        /// Get all entries.
        /// </summary>
        public IEnumerable<PersonModel> GetAll()
        {
            var elements = this.primaryIndex.GetAll();

            foreach (var entry in elements)
            {
                yield return this.personSerializer.Deserialize(this.peopleRecords.Find(entry.Item2));
            }
        }

        /// <summary>
        /// Delete specified entry from database.
        /// </summary>
        public void Delete(PersonModel cow)
        {
            throw new NotImplementedException();
        }
        #endregion Methods (public)

        #region Dispose
        bool disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing && !disposed)
            {
                this.mainDatabaseFile.Dispose();
                this.primaryIndexFile.Dispose();
                this.secondaryIndexFile.Dispose();
                this.disposed = true;
            }
        }

        ~PeopleDatabase()
        {
            Dispose(false);
        }
        #endregion Dispose
    }
}
