using Stowage;

namespace DeltaLake.Kernel.Exceptions {
    public class TableNotFoundException : KernelException {
        public TableNotFoundException(IOPath tablePath) : base($"Delta table at path `{tablePath}` is not found.") {
            TablePath = tablePath;
        }

        public IOPath TablePath { get; }
    }
}
