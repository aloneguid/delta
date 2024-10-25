namespace DeltaLake.Log.Actions {
    public class ChangeMetadataAction : Action {

        private readonly ChangeMetadataPoco _data;

        internal ChangeMetadataAction(ChangeMetadataPoco data) : base(DeltaAction.Metadata) {
            _data = data;

            if(data.Id == null)
                throw new ArgumentNullException(nameof(data.Id));

            Id = Guid.Parse(data.Id);
            Name = data.Name;
        }

        public Guid Id { get; }

        public string? Name { get; }
    }
}
