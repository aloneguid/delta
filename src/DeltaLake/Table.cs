using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using DeltaLake.Log;
using DeltaLake.Log.Actions;
using Stowage;

namespace DeltaLake {
    public class Table : IAsyncDisposable {
        private readonly IFileStorage _storage;
        private readonly ICachedStorage _fileStorage;
        private readonly IOPath _location;
        private readonly bool _disposeStorage;
        private readonly List<DataFile> _dataFiles = new();

        private Table(IFileStorage storage, IOPath location,
            DeltaLog log, IReadOnlyCollection<LogCommit> history,
            bool disposeStorage = false) {
            _storage = storage;
            _fileStorage = Files.Of.MemoryCacheStorage(storage, location.ToString());
            _location = location;
            _disposeStorage = disposeStorage;
            Log = log;
            History = history;
            Metadata = FindMetadata();

            FindDataFiles();
        }

        /// <summary>
        /// Opens an existing Delta Lake table
        /// </summary>
        /// <param name="storage"></param>
        /// <param name="location"></param>
        /// <param name="disposeStorage"></param>
        /// <returns></returns>
        public static async Task<Table> OpenAsync(IFileStorage storage, IOPath location, bool disposeStorage = false) {

            var log = new DeltaLog(storage, location);
            IReadOnlyCollection<LogCommit> history = await log.ReadHistoryAsync();
            return new Table(storage, location, log, history, disposeStorage);
        }

        /// <summary>
        /// Table commit history
        /// </summary>
        public IReadOnlyCollection<LogCommit> History { get; private set; }

        /// <summary>
        /// List of table versions in increasing order.
        /// </summary>
        public IReadOnlyCollection<long> Versions => History.Select(c => c.Version).ToList();

        /// <summary>
        /// Current table version
        /// </summary>
        public long CurrentVersion => History.Last().Version;

        public DeltaLog Log { get; init; }

        public Metadata Metadata { get; init; }

        /// <summary>
        /// List of data files in the active version of this table
        /// </summary>
        public IReadOnlyCollection<DataFile> DataFiles => _dataFiles;

        /// <summary>
        /// Determines the list of active data files in the table at the given version.
        /// </summary>
        /// <returns></returns>
        private void FindDataFiles() {

            var files = new HashSet<DataFile>();

            foreach(LogCommit commit in History) {
                foreach(Log.Actions.Action action in commit.Actions) {
                    if(action.DeltaAction == ActionType.AddFile || action.DeltaAction == ActionType.RemoveFile) {
                        bool isAdd = action.DeltaAction == ActionType.AddFile;

                        var fb = (FileBase)action;
                        fb.Validate();

                        var fullPath = new IOPath(_location, fb.Path!);
                        DataFile dataFile = new DataFile(fb, fullPath);

                        if(isAdd) {
                            files.Add(dataFile);
                        } else {
                            files.Remove(dataFile);
                        }
                    }
                }
            }

            _dataFiles.Clear();
            _dataFiles.AddRange(files);
        }

        private Metadata FindMetadata() {
            return History
                .SelectMany(c => c.Actions)
                .Where(a => a.DeltaAction == ActionType.Metadata)
                .Cast<Metadata>()
                .Last();
        }

        public async Task<Stream> OpenSeekableStreamAsync(DataFile dataFile) {
            Stream? src = await _fileStorage.OpenRead(dataFile.Path);
            if(src == null) {
                throw new FileNotFoundException($"File not found: {dataFile.Path}");
            }
            return src;
        }

        public ValueTask DisposeAsync() {
            if(_disposeStorage) {
                _storage.Dispose();
            }
            return ValueTask.CompletedTask;
        }
    }
}
