namespace DeltaLake.Log.Actions {
    public class ProtocolEvolutionAction : Action {
        internal ProtocolEvolutionAction(ProtocolEvolutionActionPoco data) : base(DeltaAction.Protocol) {
            Data = data;
            MinReaderVersion = data.MinReaderVersion ?? 0;
            MinWriterVersion = data.MinWriterVersion ?? 0;
        }

        ProtocolEvolutionActionPoco Data { get; }

        public int MinReaderVersion { get; }

        public int MinWriterVersion { get; }
    }
}
