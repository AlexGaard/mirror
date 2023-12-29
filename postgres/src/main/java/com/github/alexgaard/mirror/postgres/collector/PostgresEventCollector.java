package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.EventSink;
import com.github.alexgaard.mirror.core.EventSource;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.postgres.collector.message.*;
import com.github.alexgaard.mirror.postgres.event.*;
import com.github.alexgaard.mirror.postgres.metadata.ColumnMetadata;
import com.github.alexgaard.mirror.postgres.metadata.ConstraintMetadata;
import com.github.alexgaard.mirror.postgres.metadata.PgDataType;
import com.github.alexgaard.mirror.postgres.metadata.PgMetadata;
import com.github.alexgaard.mirror.postgres.utils.BackgroundJob;
import com.github.alexgaard.mirror.postgres.utils.PgTimestamp;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.*;
import static com.github.alexgaard.mirror.postgres.metadata.PgMetadata.tableFullName;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.MESSAGE_PREFIX;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.SKIP_TRANSACTION_MSG;
import static com.github.alexgaard.mirror.postgres.utils.FieldMapper.mapTupleDataToField;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
import static java.lang.String.format;
import static java.util.Optional.*;

public class PostgresEventCollector implements EventSource {

    private final static Logger log = LoggerFactory.getLogger(PostgresEventCollector.class);

    private final String replicationSlotName;

    private final String publicationName;

    private final int maxChangesPrPoll = 500;

    private final String sourceName;

    private final Map<Integer, PgDataType> pgDataTypes = new HashMap<>();

    private final Map<String, List<ColumnMetadata>> tableColumnMetadata = new HashMap<>();

    private final Map<String, List<ConstraintMetadata>> tableConstraintMetadata = new HashMap<>();

    private final Map<String, TableReplicationConfig> tableReplicationConfig = new HashMap<>();

    private final DataSource dataSource;

    private final PgReplication pgReplication;

    private final BackgroundJob backgroundJob;

    private EventSink eventSink;

    public PostgresEventCollector(
            String sourceName,
            DataSource dataSource,
            Duration pollInterval,
            PgReplication pgReplication
    ) {
        this.sourceName = sourceName;
        this.dataSource = dataSource;
        this.pgReplication = pgReplication;
        this.replicationSlotName = pgReplication.getReplicationSlotName();
        this.publicationName = pgReplication.getPublicationName();
        this.backgroundJob = new BackgroundJob(
                this.getClass().getSimpleName(),
                pollInterval,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10)
        );
    }

    public void setTableReplicationConfig(String schema, String table, TableReplicationConfig config) {
        tableReplicationConfig.put(tableFullName(schema, table), config);
    }

    @Override
    public void setEventSink(EventSink eventSink) {
        if (eventSink == null) {
            throw new IllegalArgumentException("event sink cannot be null");
        }

        this.eventSink = eventSink;
    }

    @Override
    public synchronized void start() {
        if (backgroundJob.isRunning()) {
            return;
        }

        if (eventSink == null) {
            throw new IllegalStateException("cannot start without a sink to consume the events");
        }

        log.debug("Collecting metadata");

        pgDataTypes.putAll(PgMetadata.getAllPgDataTypes(dataSource));

        pgReplication.getSchemas().forEach(schema -> {
            tableColumnMetadata.putAll(PgMetadata.getAllTableColumns(dataSource, schema));
            tableConstraintMetadata.putAll(PgMetadata.getAllTableConstraints(dataSource, schema));
        });

        log.debug("Initializing postgres replication");

        pgReplication.setup(dataSource);

        log.debug("Starting event collector");

        backgroundJob.start(this::collectEventsFromWal);
    }

    @Override
    public synchronized void stop() {
        backgroundJob.stop();
    }

    private void collectEventsFromWal() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            String lastLsn = null;

            List<RawMessage> rawMessages = peekDataChanges(connection);

            if (rawMessages.isEmpty()) {
                return;
            }

            List<Message> messages = rawMessages
                    .stream()
                    .map(MessageParser::parse)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            // Relation messages are sometimes reused across transactions to save bandwidth if enough transactions use the same relation.
            List<RelationMessage> relationMessages = messages.stream()
                    .filter(m -> m instanceof RelationMessage)
                    .map(m -> (RelationMessage) m)
                    .collect(Collectors.toList());

            List<List<Message>> transactions = splitIntoTransactions(messages);

            try {
                for (List<Message> transaction : transactions) {
                    if (shouldTransactionBeSkipped(transaction)) {
                        continue;
                    }

                    List<DataChangeEvent> transactionEvents = toEventTransaction(transaction, relationMessages);

                    CommitMessage commit = (CommitMessage) transaction.stream()
                            .filter(m -> m.type.equals(Message.Type.COMMIT))
                            .findAny()
                            .orElseThrow();

                    PostgresTransactionEvent pgTransaction = PostgresTransactionEvent.of(
                            sourceName,
                            transactionEvents,
                            PgTimestamp.toOffsetDateTime(commit.commitTimestamp)
                    );

                    Result result = runWithResult(() -> eventSink.consume(pgTransaction));

                    if (result.isOk()) {
                        lastLsn = commit.lsn;
                    } else {
                        throw softenException(result.getError().get());
                    }
                }
            } finally {
                if (lastLsn != null) {
                    removeNextTransactions(connection, lastLsn);
                }
            }
        }
    }

    private List<DataChangeEvent> toEventTransaction(List<Message> msgTransaction, List<RelationMessage> relationMessages) {
        List<DataChangeEvent> event = new ArrayList<>();

        for (Message message : msgTransaction) {
            switch (message.type) {
                case INSERT: {
                    InsertMessage insert = (InsertMessage) message;

                    RelationMessage relation = relationMessages
                            .stream()
                            .filter(m -> m.oid == insert.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field<?>> fields = toFields(insert.columns, relation);

                    InsertEvent insertDataChange = new InsertEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            insert.xid,
                            fields
                    );

                    event.add(insertDataChange);
                    break;
                }
                case DELETE: {
                    DeleteMessage delete = (DeleteMessage) message;

                    RelationMessage relation = relationMessages
                            .stream()
                            .filter(m -> m.oid == delete.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field<?>> identifyingFields = findIdentifyingFields(delete.replicaIdentityType, delete.columns, relation);

                    DeleteEvent deleteEvent = new DeleteEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            delete.xid,
                            identifyingFields
                    );

                    event.add(deleteEvent);
                    break;
                }
                case UPDATE: {
                    UpdateMessage update = (UpdateMessage) message;

                    RelationMessage relation = relationMessages
                            .stream()
                            .filter(m -> m.oid == update.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field<?>> identifyingFields = findIdentifyingFields(update.replicaIdentityType, update.oldTupleOrKeyColumns, relation);
                    List<Field<?>> updatedFields = findUpdatedFields(update.replicaIdentityType, update, relation, identifyingFields);

                    UpdateEvent updateEvent = new UpdateEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            update.xid,
                            identifyingFields,
                            updatedFields
                    );

                    event.add(updateEvent);
                    break;
                }
            }

        }

        return event;
    }

    private List<Field<?>> findIdentifyingFields(Character replicaIdentityType, List<TupleDataColumn> oldColumns, RelationMessage relation) {
        if (replicaIdentityType != null && replicaIdentityType == 'O') {
            // Replica identity FULL

            String fullName = tableFullName(relation.namespace, relation.relationName);

            List<ConstraintMetadata> constraints = ofNullable(tableConstraintMetadata.get(fullName)).orElseGet(Collections::emptyList);
            List<ColumnMetadata> columns = tableColumnMetadata.get(fullName);
            Optional<TableReplicationConfig> replicationConfig = ofNullable(tableReplicationConfig.get(fullName));

            Optional<ConstraintMetadata> identifyingConstraint = getPreferredConstraint(replicationConfig, constraints)
                    .or(() -> findIdentifyingConstraint(constraints, columns));

            return identifyingConstraint
                    .map(constraintMetadata -> toFieldsV2(oldColumns, relation,
                            (field, ordinalPos) -> constraintMetadata.constraintKeyOrdinalPositions.contains(ordinalPos)))
                    .orElseGet(() -> toFields(oldColumns, relation));
        }

        return toFields(oldColumns, relation)
                .stream()
                .filter(f -> relation.columns.stream().anyMatch(c -> c.name.equals(f.name) && c.partOfKey))
                .collect(Collectors.toList());
    }

    private List<Field<?>> findUpdatedFields(
            Character replicaIdentityType,
            UpdateMessage update,
            RelationMessage relation,
            List<Field<?>> identifyingFields
    ) {
        if (replicaIdentityType != null && replicaIdentityType == 'K') {
            // Remove fields that are part of key and have not changed.
            // This can happen when using a composite key and only some of the columns are changed.

            return toFields(update.columnsAfterUpdate, relation)
                    .stream()
                    .filter(f -> !identifyingFields.contains(f))
                    .collect(Collectors.toList());
        } else if (update.replicaIdentityType != null && update.replicaIdentityType == 'O') {
            // Filter out columns that have not changed

            List<Field<?>> oldColumns = toFields(update.oldTupleOrKeyColumns, relation);

            return toFields(update.columnsAfterUpdate, relation)
                    .stream()
                    .filter(newCol -> !oldColumns.contains(newCol))
                    .collect(Collectors.toList());
        } else {
            // Filter out columns that are part of key which have not changed

            return toFields(update.columnsAfterUpdate, relation)
                    .stream()
                    .filter(f -> relation.columns.stream()
                            .filter(c -> c.name.equals(f.name))
                            .findAny().map(c -> !c.partOfKey).orElse(true)
                    )
                    .collect(Collectors.toList());
        }
    }

    private static Optional<ConstraintMetadata> getPreferredConstraint(Optional<TableReplicationConfig> maybeConfig, List<ConstraintMetadata> constraints) {
        return maybeConfig.map(config -> {
            if (config.preferredConstraint == null) {
                return null;
            }

            return constraints.stream().filter(c -> config.preferredConstraint.equals(c.constraintName))
                    .findAny()
                    .orElseThrow(() -> new IllegalStateException("Unable to find constraint with name: " + config.preferredConstraint));
        });
    }

    private static Optional<ConstraintMetadata> findIdentifyingConstraint(
            List<ConstraintMetadata> constraints,
            List<ColumnMetadata> columns
    ) {
        for (ConstraintMetadata constraint : constraints) {
            if (constraint.type.equals(ConstraintMetadata.ConstraintType.PRIMARY_KEY)) {
                return of(constraint);
            }

            // Uses the first constraint without nullable fields as the identifier
            if (constraint.type.equals(ConstraintMetadata.ConstraintType.UNIQUE)) {
                boolean hasNullableField = columns
                        .stream()
                        .anyMatch(c -> constraint.constraintKeyOrdinalPositions.contains(c.ordinalPosition) && c.isNullable);

                if (!hasNullableField) {
                    return of(constraint);
                }
            }
        }

        return empty();
    }

    private List<Field<?>> toFields(List<TupleDataColumn> columns, RelationMessage relation) {
        if (columns.size() > relation.columns.size()) {
            throw new IllegalArgumentException(format("Tuple data columns length (%d) must be equal or less than relation columns (%d)", columns.size(), relation.columns.size()));
        }

        List<Field<?>> fields = new ArrayList<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            TupleDataColumn insertCol = columns.get(i);
            RelationMessage.Column relationCol = relation.columns.get(i);
            Field.Type type = pgDataTypes.get(relationCol.dataOid).getType();

            fields.add(mapTupleDataToField(relationCol.name, type, insertCol));
        }

        return fields;
    }

    private List<Field<?>> toFieldsV2(List<TupleDataColumn> columns, RelationMessage relation, FieldFilter filter) {
        if (columns.size() > relation.columns.size()) {
            throw new IllegalArgumentException(format("Tuple data columns length (%d) must be equal or less than relation columns (%d)", columns.size(), relation.columns.size()));
        }

        List<Field<?>> fields = new ArrayList<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            TupleDataColumn insertCol = columns.get(i);
            RelationMessage.Column relationCol = relation.columns.get(i);
            Field.Type type = pgDataTypes.get(relationCol.dataOid).getType();

            Field<?> field = mapTupleDataToField(relationCol.name, type, insertCol);

            if (filter.filterField(field, i + 1)) {
                fields.add(field);
            }
        }

        return fields;
    }

    private interface FieldFilter {

        boolean filterField(Field<?> field, int ordinalPos);

    }

    private void removeNextTransactions(Connection connection, String upToLsn) {
        String sql = "SELECT 1 FROM pg_logical_slot_get_binary_changes(?, ?, NULL, 'messages', 'true', 'proto_version', '1', 'publication_names', ?)";

        query(connection, sql, statement -> {
            PGobject obj = new PGobject();
            obj.setType("pg_lsn");
            obj.setValue(upToLsn);

            statement.setString(1, replicationSlotName);
            statement.setObject(2, obj);
            statement.setString(3, publicationName);
            statement.executeQuery();
        });
    }

    private List<RawMessage> peekDataChanges(Connection connection) {
        String sql = "SELECT * FROM pg_logical_slot_peek_binary_changes(?, NULL, ?, 'messages', 'true', 'proto_version', '1', 'publication_names', ?)";

        return query(connection, sql, (statement) -> {
            statement.setString(1, replicationSlotName);
            statement.setInt(2, maxChangesPrPoll);
            statement.setString(3, publicationName);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, PostgresEventCollector::toRawEvent);
        });
    }

    private static List<List<Message>> splitIntoTransactions(List<Message> messages) {
        List<List<Message>> transactions = new ArrayList<>();
        List<Message> lastList = null;
        int currentXid = -1;

        for (Message m : messages) {
            if (m.xid != currentXid) {
                lastList = new ArrayList<>();
                transactions.add(lastList);
                currentXid = m.xid;
            }

            if (lastList != null) {
                lastList.add(m);
            }
        }

        return transactions;
    }

    private static RawMessage toRawEvent(ResultSet resultSet) throws SQLException {
        String lsn = resultSet.getString("lsn");
        int xid = resultSet.getInt("xid");
        byte[] data = resultSet.getBytes("data");

        return new RawMessage(lsn, xid, data);
    }

    private static boolean shouldTransactionBeSkipped(List<Message> transactionMessages) {
        return transactionMessages.stream().anyMatch(m -> {
            if (!(m instanceof LogicalDecodingMessage)) {
                return false;
            }

            LogicalDecodingMessage logicalDecodingMessage = (LogicalDecodingMessage) m;

            return MESSAGE_PREFIX.equals(logicalDecodingMessage.prefix) &&
                    SKIP_TRANSACTION_MSG.equals(new String(logicalDecodingMessage.content));
        });
    }

}
