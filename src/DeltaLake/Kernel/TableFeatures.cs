using DeltaLake.Kernel.Exceptions;
using DeltaLake.Kernel.Internal.Actions;
using DeltaLake.Kernel.Util;
using Stowage;

namespace DeltaLake.Kernel {
    static class TableFeatures {
        private static readonly HashSet<string> SupportedReaderFeatures = new HashSet<string> {
            "columnMapping",
            "deletionVectors",
            "timestampNtz",
            "typeWidening-preview",
            "typeWidening",
            "vacuumProtocolCheck",
            "variantType-preview",
            "v2Checkpoint"
        };

        public static void ValidateReadSupportedTable(Protocol protocol, IOPath tablePath, Metadata? metadata) {
            switch(protocol.MinReaderVersion) {
                case 1:
                    break;
                case 2:
                    if(metadata != null) {
                        ColumnMapping.ThrowOnUnsupportedColumnMappingMode(metadata);
                    }
                    break;
                case 3:
                    if(protocol?.ReaderFeatures != null) {
                        var unsupportedFeatures = new List<string>();
                        foreach(string feature in protocol.ReaderFeatures) {
                            if(!SupportedReaderFeatures.Contains(feature)) {
                                unsupportedFeatures.Add(feature);
                            }
                        }
                        if(unsupportedFeatures.Any()) {
                            throw new KernelException($"Unsupported Delta reader features: table `{tablePath}` requires reader table features [{string.Join(", ", unsupportedFeatures)}] which is unsupported by this version of Delta Kernel.");
                        }
                        if(protocol.ReaderFeatures.Contains("columnMapping")) {
                            if(metadata != null) {
                                ColumnMapping.ThrowOnUnsupportedColumnMappingMode(metadata);
                            }
                        }

                    }
                    break;
                default:
                    throw new KernelException($"Unsupported Delta protocol reader version: table `{tablePath}` requires reader version {protocol.MinReaderVersion} which is unsupported by this version of Delta Kernel.");
            }
        }
    }
}
