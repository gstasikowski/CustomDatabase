using CustomDatabase.Logic;
using CustomDatabase.TestApp.Models;

namespace CustomDatabase.TestApp.Logic
{
    class PeopleDatabase : IDisposable
    {
        #region Variables
        const int BlockSize = 4096;
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
            _mainDatabaseFile = new FileStream(
                path: pathToDBFile,
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: BlockSize
            );
            _primaryIndexFile = new FileStream(
                path: pathToDBFile + ".pidx",
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: BlockSize
            );
            _secondaryIndexFile = new FileStream(
                path: pathToDBFile + ".sidx",
                mode: FileMode.OpenOrCreate,
                access: FileAccess.ReadWrite,
                share: FileShare.None,
                bufferSize: BlockSize
            );

            // Create a RecordStorage for main cow data
            _peopleRecords = new RecordStorage(
                new BlockStorage(storage: _mainDatabaseFile, blockSize: BlockSize, blockHeaderSize: 48));

            // Create the indexes
            _primaryIndex = new Tree<Guid, uint>(
                nodeManager: new TreeDiskNodeManager<Guid, uint>(
                    keySerializer: new GuidSerializer(),
                    valueSerializer: new TreeUIntSerializer(),
                    nodeStorage: new RecordStorage(
                        new BlockStorage(storage: _primaryIndexFile, blockSize: BlockSize)
                    )
                ),
                allowDuplicateKeys: false
            );

            _secondaryIndex = new Tree<Tuple<string, string>, uint>(
				nodeManager: new TreeDiskNodeManager<Tuple<string, string>, uint>(
					keySerializer: new StringSerializer(), 
					valueSerializer: new TreeUIntSerializer(), 
					nodeStorage: new RecordStorage(
                        new BlockStorage(storage: _secondaryIndexFile, blockSize: BlockSize))
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

            uint recordId = _peopleRecords.Create(_personSerializer.Serialize(person));

            _primaryIndex.Insert(key: person.Id, value: recordId);
            _secondaryIndex.Insert(
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

            var entry = _primaryIndex.Get(id);

            if (entry == null)
            {
                return null;
            }

            return _personSerializer.Deserialize(_peopleRecords.Find(entry.Item2));
        }

        /// <summary>
        /// Find all entries within parameteres.
        /// </summary>
        public IEnumerable<PersonModel> FindBy(string firstName, string lastName)
        {
            var comparer = Comparer<Tuple<string, string>>.Default;
            var searchKey = new Tuple<string, string>(item1: firstName, item2: lastName);

            foreach (var entry in _secondaryIndex.LargerThanOrEqualTo(searchKey))
            {
                // Stop upon reaching key larger than provided
                if (comparer.Compare(entry.Item1, searchKey) > 0)
                {
                    break;
                }

                yield return _personSerializer.Deserialize(_peopleRecords.Find(entry.Item2));
            }
        }

        /// <summary>
        /// Get all entries.
        /// </summary>
        public IEnumerable<PersonModel> GetAll()
        {
            var elements = _primaryIndex.GetAll();

            foreach (var entry in elements)
            {
                yield return _personSerializer.Deserialize(_peopleRecords.Find(entry.Item2));
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
                _mainDatabaseFile.Dispose();
                _primaryIndexFile.Dispose();
                _secondaryIndexFile.Dispose();
                disposed = true;
            }
        }

        ~PeopleDatabase()
        {
            Dispose(false);
        }
        #endregion Dispose
    }
}