namespace DeltaLake.Log.Actions {
    public class ChangeMetadataAction : Action {

        private readonly ChangeMetadataPoco _data;

        internal ChangeMetadataAction(ChangeMetadataPoco data) : base(DeltaAction.Metadata) {
            _data = data;
            Id = data.Id;
            Name = data.Name;
        }

        public Guid Id { get; }

        public string? Name { get; }
    }
}
