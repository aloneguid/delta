using Stowage;

namespace DeltaLake.Kernel.Engine {
    public class DefaultEngine : IEngine {

        public DefaultEngine(IFileStorage fileStorage) {
            FileStorage = fileStorage;
        }

        public IFileStorage FileStorage { get; }

        public async Task<IReadOnlyCollection<IOEntry>> ListFrom(IOPath path, string prefix) {
            IReadOnlyCollection<IOEntry> entries = await FileStorage.Ls(path);

            return entries
                .Where(e => e.Path.IsFile)
                .OrderBy(e => e.Name)
                .Where(e => e.Name.CompareTo(prefix) > 0)
                .ToList();
        }
    }
}
