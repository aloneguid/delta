using Stowage;

namespace DeltaLake.Kernel.Exceptions {

    /// <summary>
    /// Thrown when an invalid table is encountered; the table's log and/or checkpoint files are in an invalid state.
    /// </summary>
    public class InvalidTableException : Exception {
        public InvalidTableException(IOPath tablePath, string message) : 
            base($"Invalid table found at {tablePath}: {message}") { }
    }
}
