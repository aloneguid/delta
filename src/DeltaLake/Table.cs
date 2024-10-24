using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DeltaLake.Log;
using Stowage;

namespace DeltaLake {
    public class Table {
        private readonly IFileStorage _storage;
        private readonly IOPath _location;

        private Table(IFileStorage storage, IOPath location) {
            _storage = storage;
            _location = location;
            Log = new DeltaLog(storage, location);
        }

        public DeltaLog Log { get; }

        private async Task OpenAsync() {
            await Log.OpenAsync();
        }

        public static async Task<Table> OpenAsync(IFileStorage storage, IOPath location) {
            var r = new Table(storage, location);
            await r.OpenAsync();
            return r;
        }

    }
}
