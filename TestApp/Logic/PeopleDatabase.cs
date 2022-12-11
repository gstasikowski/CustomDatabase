using CustomDatabase.Logic;
using CustomDatabase.Logic.Tree;
using TestApp.Models;

namespace TestApp.Logic
{
    class PeopleDatabase : IDisposable
    {
        #region Variables
        private readonly Stream _mainDatabaseFile;
        private readonly Stream _primaryIndexFile;
        private readonly Stream _secondaryIndexFile;
        private readonly Tree<Guid, uint> _primaryIndex;
        private readonly Tree<Tuple<string, string>, uint> _secondaryIndex;
        private readonly RecordStorage _peopleRecords;
        private readonly PersonSerializer _personSerializer = new PersonSerializer();
        #endregion Variables

        #region Constructor
        public PeopleDatabase(string pathToDBFile)
        {
            if (pathToDBFile == null)
            {
                throw new ArgumentNullException("pathToDBFile");
            }

            // Open the stream and (create) database files.
            this._mainDatabaseFile = new FileStream(
                path: pathToDBFile,
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: 4096
            );
            this._primaryIndexFile = new FileStream(
                path: pathToDBFile + ".pidx",
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: 4096
            );
            this._secondaryIndexFile = new FileStream(
                path: pathToDBFile + ".sidx",
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: 4096
            );

            // Create a RecordStorage for main cow data
            this._peopleRecords = new RecordStorage(
                new BlockStorage(storage: this._mainDatabaseFile, blockSize: 4096, blockHeaderSize: 48));

            // Create the indexes
            this._primaryIndex = new Tree<Guid, uint>(
                nodeManager: new TreeDiskNodeManager<Guid, uint>(
                    keySerializer: new GuidSerializer(),
                    valueSerializer: new TreeUIntSerializer(),
                    nodeStorage: new RecordStorage(
                        new BlockStorage(storage: this._primaryIndexFile, blockSize: 4096)
                    )
                ),
                allowDuplicateKeys: false
            );

            this._secondaryIndex = new Tree<Tuple<string, string>, uint>(
				nodeManager: new TreeDiskNodeManager<Tuple<string, string>, uint>(
					keySerializer: new StringSerializer(), 
					valueSerializer: new TreeUIntSerializer(), 
					nodeStorage: new RecordStorage(
                        new BlockStorage(storage: this._secondaryIndexFile, blockSize: 4096))
                ),
                allowDuplicateKeys: true
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
            {
                throw new ObjectDisposedException("PeopleDatabase");
            }

            throw new NotImplementedException();
        }

        /// <summary>
        /// Insert new entry into database.
        /// </summary>
        public void Insert(PersonModel person)
        {
            if (disposed)
            {
                throw new ObjectDisposedException("PeopleDatabase");
            }

            uint recordId = this._peopleRecords.Create(this._personSerializer.Serialize(person));

            this._primaryIndex.Insert(key: person.Id, value: recordId);
            this._secondaryIndex.Insert(
                key: new Tuple<string, string>(item1: person.FirstName, item2: person.LastName),
                value: recordId
            );
        }

        /// <summary>
        /// Find an entry by ID.
        /// </summary>
        public PersonModel Find(Guid id)
        {
            if (disposed)
            {
                throw new ObjectDisposedException("PeopleDatabase");
            }

            var entry = this._primaryIndex.Get(id);

            if (entry == null)
            {
                return null;
            }

            return this._personSerializer.Deserialize(this._peopleRecords.Find(entry.Item2));
        }

        /// <summary>
        /// Find all entries within parameteres.
        /// </summary>
        public IEnumerable<PersonModel> FindBy(string firstName, string lastName)
        {
            var comparer = Comparer<Tuple<string, string>>.Default;
            var searchKey = new Tuple<string, string>(item1: firstName, item2: lastName);

            foreach (var entry in this._secondaryIndex.LargerThanOrEqualTo(searchKey))
            {
                // Stop upon reaching key larger than provided
                if (comparer.Compare(entry.Item1, searchKey) > 0)
                {
                    break;
                }

                yield return this._personSerializer.Deserialize(this._peopleRecords.Find(entry.Item2));
            }
        }

        /// <summary>
        /// Get all entries.
        /// </summary>
        public IEnumerable<PersonModel> GetAll()
        {
            var elements = this._primaryIndex.GetAll();

            foreach (var entry in elements)
            {
                yield return this._personSerializer.Deserialize(this._peopleRecords.Find(entry.Item2));
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
                this._mainDatabaseFile.Dispose();
                this._primaryIndexFile.Dispose();
                this._secondaryIndexFile.Dispose();
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