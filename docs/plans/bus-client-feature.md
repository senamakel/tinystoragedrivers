# Optional bus client implementation

Spec: [Optional TinyBus client in the storage contract](../specs/bus-client-feature.md).

## Ordered work

1. Make the existing `DocumentClient` dependency optional behind a default-on
   `client` feature; gate its module and re-export with that feature.
2. Add an independent no-default-features consumer that constructs all fields
   of the canonical `StorageNeeds` schema and rejects forbidden dependencies
   from its resolved normal/build graph.
3. Run the no-default-features consumer proof, the default bus tests, and the
   existing compiled document-callback proof. Add the consumer proof to CI and
   document both Cargo dependency forms.
