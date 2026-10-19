# Existing schema storage handles

An optional native backend factory returns handles that do not declare, migrate or repair schema. The policy survives native fenced constructors. Unsupported implementations refuse; existing unrestricted callers retain their behavior.

Document validation checks actual persisted declaration presence and compatibility (stored.merge(required) equals stored). Absence is NotFound, malformed or incompatible requests InvalidInput, corrupt metadata Serialization. Native errors retain sources. Restricted ensure refuses; scoped drop_collection remains fenced data deletion.

Internal stream/blob records and data files are allowed data mutations. Native index/base-table preparation is trusted bootstrap. Mongo restricted operations inspect existing native schema rather than create indexes, including reads. External concurrent schema administration is unsupported. Reopened handles require persisted evidence rather than driver names or caller flags.

Every supported native write retains its captured fence and atomic guard check. Mongo GridFS fencing remains unsupported. Driver defaults are Unsupported; no workspace fallback is provided.

## Public API and native limits

StorageBackend::prepare_schema is optional trusted async bootstrap; its default refuses. It prepares internal storage only, never application document declarations. Memory and File require no extra internal schema initialization. SQLite prepares its base tables; Mongo prepares document-validation metadata, stream heads/segments and GridFS files/chunks/indexes. Restricted document writes require the validation collection to exist; scope-registration records remain internal data mutations. Checked batch read dependencies also require persisted declarations. Existing-schema backends refuse this method, even for matching schema.

StorageBackend::existing_schema is a synchronous optional native factory with default Unsupported(ExistingSchema). DocumentStore::validate_collection_spec defaults to Unsupported(CollectionValidation). The native factory does not accept a caller-supplied preparation boolean or change a fence. Classic opening/constructors retain their behavior: the restriction starts when existing_schema is called, so this does not retroactively remove trusted initialization performed before that call.

SQLite validates the driver's table layout of the already open file without create flags or journal-mode writes. Mongo uses fresh declaration and index-catalog reads rather than the classic declaration/preparation caches. Memory preserves the actual shared native state.

Restricted Mongo GridFS uploads refuse Unsupported(ExistingSchema): the SDK may issue create_collection before checking existing indexes. Its native fenced writes retain their existing Unsupported(Fencing). Existing blob reads and unfenced deletion use read-only schema checks. No presigned-upload or cross-driver purge authority is introduced.

Trusted GridFS preparation creates the unique `(files_id, n)` chunk index; restricted blob operations validate that uniqueness rather than treating a non-unique index as prepared schema. Standalone scoped document deletion validates the persisted declaration and its indexes before changing records. Restricted blob operations also require the standard `(filename, uploadDate)` files index. Memory, File and SQLite validate operation inputs, including IDs, document bodies, filters, sort fields, cursors and batch dependencies, before checking declaration presence; malformed inputs remain InvalidInput even when no declaration exists.

Restricted Mongo stream windows validate persisted heads, segments and required indexes even when the requested limit is zero. Returning an empty window does not waive schema admission.

An administrator changing or deleting schema concurrently is outside this contract. The owner bootstrap and restricted data handles must address the same real native database; inspection is not authority to repair missing schema. Generation-aware native guard bindings preserve the existing-schema restriction. Trusted Mongo bootstrap also prepares purge receipt and blob-manifest collections; restricted purge requires both collections to exist before it writes reset metadata. Trusted bootstrap for a composite record backend must likewise prepare the `__tsd_composite_purges` declaration. Composite purge under an ExistingSchema handle validates that declaration and fails closed if it is absent or incompatible; it does not create or repair the coordinator schema. Coordinator documents and their receipts remain internal and are excluded from public scope enumeration.
