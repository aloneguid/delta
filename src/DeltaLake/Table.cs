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
    public class Table : IDisposable {
        private readonly IFileStorage _storage;
        private readonly ICachedStorage _fileStorage;
        private readonly IOPath _location;
        private readonly bool _disposeStorage;
        private IReadOnlyCollection<LogCommit>? _history;

        public Table(IFileStorage storage, IOPath location, bool disposeStorage = false) {
            _storage = storage;
            _fileStorage = Files.Of.MemoryCacheStorage(storage, location.ToString());
            _location = location;
            _disposeStorage = disposeStorage;
            Log = new DeltaLog(storage, location);
        }

        public static async Task<bool> IsDeltaTableAsync(IFileStorage storage, IOPath location) {
            IOPath logLocation = location.Combine(DeltaLog.DeltaLogDirName);

            IReadOnlyCollection<IOEntry> items = await storage.Ls(logLocation, false);

            return items.Count > 0;
        }

        public DeltaLog Log { get; init; }

        private async Task<IReadOnlyCollection<LogCommit>> GetOrFetchHistoryAsync() {
            if(_history == null) {
                _history = await Log.ReadHistoryAsync();
            }

            return _history;
        }

        /// <summary>
        /// Opens delta table from disk at given location.
        /// </summary>
        /// <param name="directoryPath"></param>
        /// <returns></returns>
        public static Table OpenFromDisk(string directoryPath) {
            if(!Directory.Exists(directoryPath)) {
                throw new DirectoryNotFoundException($"Directory not found: {directoryPath}");
            }

            var di = new DirectoryInfo(directoryPath);
            IFileStorage storage = Files.Of.LocalDisk(di.Parent!.FullName);
            return new Table(storage, new IOPath(di.Name));
        }

        /// <summary>
        /// Lists table versions in increasing order.
        /// </summary>
        /// <returns></returns>
        public async Task<IReadOnlyCollection<long>> ListVersionsAsync() {
            IReadOnlyCollection<LogCommit> history = await GetOrFetchHistoryAsync();
            return history.Select(c => c.Version).ToList();
        }

        /// <summary>
        /// Gets current table version
        /// </summary>
        /// <returns></returns>
        public async Task<long> GetVersionAsync() {
            return (await GetOrFetchHistoryAsync()).Last().Version;
        }

        /// <summary>
        /// Determines the list of active data files in the table at the given version.
        /// </summary>
        /// <returns></returns>
        public async Task<IReadOnlyCollection<DataFile>> GetDataFilesAsync() {

            IReadOnlyCollection<LogCommit> history = await GetOrFetchHistoryAsync();

            var files = new HashSet<DataFile>();

            foreach(LogCommit commit in history) {
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

            return files;
        }

        public async Task<Stream> OpenSeekableStreamAsync(DataFile dataFile) {
            Stream? src = await _fileStorage.OpenRead(dataFile.Path);
            if(src == null) {
                throw new FileNotFoundException($"File not found: {dataFile.Path}");
            }
            return src;
        }

        public void Dispose() {
            if(_disposeStorage) {
                _storage.Dispose();
            }
        }
    }
}
