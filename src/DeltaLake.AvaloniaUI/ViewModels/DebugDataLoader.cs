using System.Threading.Tasks;
using Stowage;

namespace DeltaLake.AvaloniaUI.ViewModels {

#if DEBUG
    static class DebugDataLoader {
        public static async Task<Table> LoadTableAsync() {
            const string path = "D:\\delta-dotnet\\src\\DeltaLake.Test\\data\\chinook\\track.partitioned.mediatypeid";
            IFileStorage fs = Files.Of.LocalDisk(path);
            Table table = await Table.OpenAsync(fs, "/");
            return table;
        }
    }
#endif
}
