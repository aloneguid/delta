namespace DeltaLake.Kernel.Internal.Actions {

    public abstract class Action {
        public ActionType DeltaAction { get; set; }

        protected Action(ActionType action) {
            DeltaAction = action;
        }

        public override string ToString() => DeltaAction.ToString().ToLower();
    }
}
