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
    public class Table {
        private readonly IFileStorage _storage;
        private readonly IOPath _location;
        private IReadOnlyCollection<LogCommit>? _history;

        public Table(IFileStorage storage, IOPath location) {
            _storage = storage;
            _location = location;
            Log = new DeltaLog(storage, location);
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
                        var adf = new DataFile(fb.Path!,
                            fb.Size == null ? 0 : fb.Size.Value,
                            fb.PartitionValues,
                            fb.Timestamp == null ? 0 : fb.Timestamp.Value);

                        if(isAdd) {
                            files.Add(adf);
                        } else {
                            files.Remove(adf);
                        }
                    }
                }
            }

            return files;
        }
    }
}
