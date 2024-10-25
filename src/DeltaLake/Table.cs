using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DeltaLake.Log;
using DeltaLake.Log.Actions;
using Stowage;

namespace DeltaLake {
    public class Table {
        private readonly IFileStorage _storage;
        private readonly IOPath _location;

        public Table(IFileStorage storage, IOPath location) {
            _storage = storage;
            _location = location;
            Log = new DeltaLog(storage, location);
        }

        public DeltaLog Log { get; init; }

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
        public async Task<IReadOnlyCollection<string>> GetFilesAsync() {

            IReadOnlyCollection<LogCommit> history = await Log.ReadHistoryAsync();

            var files = new HashSet<string>();

            foreach(LogCommit commit in history) {
                foreach(Log.Actions.Action action in commit.Actions) {

                    if(action is AddFileAction afa) {
                        files.Add(afa.Path);
                    } else if(action is RemoveFileAction rfa) {
                        files.Remove(rfa.Path);
                    }
                }
            }

            return files;
        }
    }
}
