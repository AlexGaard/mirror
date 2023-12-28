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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.runWithResult;
import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.safeRunnable;
import static com.github.alexgaard.mirror.postgres.metadata.PgMetadata.tableFullName;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.MESSAGE_PREFIX;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.SKIP_TRANSACTION_MSG;
import static com.github.alexgaard.mirror.postgres.utils.FieldMapper.mapTupleDataToField;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
import static java.lang.String.format;

public class PostgresEventCollector implements EventSource {

    private final Logger log = LoggerFactory.getLogger(PostgresEventCollector.class);

    private final String replicationSlotName;

    private final String publicationName;

    private final int maxChangesPrPoll = 500;

    private final long backoffIncreaseMs = 1000;

    private final long maxBackoffMs = 10_000;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final String sourceName;

    private final Map<Integer, PgDataType> pgDataTypes = new HashMap<>();

    private final Map<String, List<ColumnMetadata>> tableColumnMetadata = new HashMap<>();

    private final Map<String, List<ConstraintMetadata>> tableConstraintMetadata = new HashMap<>();

    private final DataSource dataSource;

    private final Duration dataChangePollInterval;

    private final PgReplication pgReplication;

    private EventSink eventSink;

    private boolean isStarted = false;

    private long currentBackoffMs = 0;

    private ScheduledFuture<?> pollSchedule;

    public PostgresEventCollector(String sourceName, DataSource dataSource, Duration pollInterval, PgReplication pgReplication) {
        this.sourceName = sourceName;
        this.dataSource = dataSource;
        this.dataChangePollInterval = pollInterval;
        this.pgReplication = pgReplication;
        this.replicationSlotName = pgReplication.getReplicationSlotName();
        this.publicationName = pgReplication.getPublicationName();
    }

    public PostgresEventCollector(String sourceName, DataSource dataSource, PgReplication pgReplication) {
        this(sourceName, dataSource, Duration.ofSeconds(1), pgReplication);
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
        if (isStarted) {
            return;
        }

        if (eventSink == null) {
            throw new IllegalStateException("cannot start without a sink to consume the events");
        }

        isStarted = true;

        log.debug("Collecting metadata");

        pgDataTypes.putAll(PgMetadata.getAllPgDataTypes(dataSource));

        pgReplication.getSchemas().forEach(schema -> {
            tableColumnMetadata.putAll(PgMetadata.getAllTableColumns(dataSource, schema));
            tableConstraintMetadata.putAll(PgMetadata.getAllTableConstraints(dataSource, schema));
        });

        log.debug("Initializing postgres replication");

        pgReplication.setup(dataSource);

        log.debug("Starting event collector");

        pollSchedule = executorService.scheduleWithFixedDelay(
                safeRunnable(this::checkForDataChanges),
                dataChangePollInterval.toMillis(),
                dataChangePollInterval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public synchronized void stop() {
        if (!isStarted) {
            return;
        }

        isStarted = false;

        if (pollSchedule != null && !pollSchedule.isDone()) {
            pollSchedule.cancel(false);
        }
    }

    private void checkForDataChanges() throws InterruptedException {
        try (Connection connection = dataSource.getConnection()) {
            String lastLsn = null;

            try {
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

                for (List<Message> transaction : transactions) {
                    if (shouldTransactionBeSkipped(transaction)) {
                        continue;
                    }

                    List<DataChangeEvent> transactionEvents = toEventTransaction(transaction, relationMessages);

                    CommitMessage commit = (CommitMessage) transaction.stream()
                            .filter(m -> m.type.equals(Message.Type.COMMIT))
                            .findAny()
                            .orElseThrow();

                    var pgTransaction = PostgresTransactionEvent.of(
                            sourceName,
                            transactionEvents,
                            PgTimestamp.toOffsetDateTime(commit.commitTimestamp)
                    );

                    Result result = runWithResult(() -> eventSink.consume(pgTransaction));

                    if (result.isOk()) {
                        lastLsn = commit.lsn;
                    } else {
                        if (lastLsn != null) {
                            removeNextTransactions(connection, lastLsn);
                        }

                        log.error("Caught exception while consuming transaction", result.getError().get());

                        currentBackoffMs = Math.min(currentBackoffMs + backoffIncreaseMs, maxBackoffMs);
                        Thread.sleep(currentBackoffMs);
                        return;
                    }
                }

                if (lastLsn != null) {
                    removeNextTransactions(connection, lastLsn);
                }
                currentBackoffMs = 0;
            } catch (Exception e) {
                log.error("Caught exception while retrieving WAL messages", e);
                if (lastLsn != null) {
                    removeNextTransactions(connection, lastLsn);
                }

                // TODO: Maybe move backoff into safeRunnable()
                currentBackoffMs = Math.min(currentBackoffMs + backoffIncreaseMs, maxBackoffMs);
                Thread.sleep(currentBackoffMs);
            }
        } catch (Exception e) {
            log.error("Uncaught exception while retrieving WAL messages", e);
            currentBackoffMs = Math.min(currentBackoffMs + backoffIncreaseMs, maxBackoffMs);
            Thread.sleep(currentBackoffMs);
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

                    List<Field<?>> fields = toFields(delete.columns, relation);

                    DeleteEvent deleteEvent = new DeleteEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            delete.xid,
                            fields
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

                    List<Field<?>> identifyingFields;
                    List<Field<?>> updatedFields;

                    if (update.replicaIdentityType != null && update.replicaIdentityType == 'K') {
                        // Identifying field was changed
                        identifyingFields = toFields(update.oldTupleOrPkColumns, relation)
                                .stream()
                                .filter(f -> relation.columns.stream().anyMatch(c -> c.name.equals(f.name) && c.partOfKey))
                                .collect(Collectors.toList());

                        updatedFields = toFields(update.columnsAfterUpdate, relation)
                                .stream()
                                .filter(f -> !identifyingFields.contains(f)) // Remove fields that have not changed
                                .collect(Collectors.toList());
                    } else if (update.replicaIdentityType != null && update.replicaIdentityType == 'O') {
                        // TODO: Check if pk or unique idx is available. Use if possible, if not default to normal FULL

                        String fullName = tableFullName(relation.namespace, relation.relationName);

                        var constraints = tableConstraintMetadata.get(fullName);
                        var columns = tableColumnMetadata.get(fullName);

                        var maybePkConstraint = constraints
                                .stream()
                                .filter(c -> c.type.equals(ConstraintMetadata.ConstraintType.PRIMARY_KEY))
                                .findAny();

                        var uniqueConstraints = constraints
                                .stream()
                                .filter(c -> c.type.equals(ConstraintMetadata.ConstraintType.UNIQUE))
                                .collect(Collectors.toList());

                        if (maybePkConstraint.isPresent()) {
                            ConstraintMetadata pkConstraint = maybePkConstraint.get();
                            identifyingFields = toFieldsV2(
                                    update.oldTupleOrPkColumns,
                                    relation,
                                    Optional.of(pkConstraint.constraintKeyOrdinalPositions)
                            );
                        } else if (uniqueConstraints.size() == 1) {
                            ConstraintMetadata uniqueConstraint = uniqueConstraints.get(0);

                            identifyingFields = toFieldsV2(
                                    update.oldTupleOrPkColumns,
                                    relation,
                                    Optional.of(uniqueConstraint.constraintKeyOrdinalPositions)
                            );
                        } else if (uniqueConstraints.size() > 1) {
                            throw new IllegalStateException("Unable to choose which constraint to use");
                        } else {
                            identifyingFields = toFields(update.oldTupleOrPkColumns, relation);
                        }

                        // TODO: Find diff between new and old for updatedFields

                        // REPLICA IDENTITY = FULL, all the old fields must be used together as a key
//                        identifyingFields = toFields(update.oldTupleOrPkColumns, relation);
                        updatedFields = toFields(update.columnsAfterUpdate, relation);
                    } else {
                        identifyingFields = toFields(update.columnsAfterUpdate, relation)
                                .stream()
                                .filter(f -> relation.columns.stream().anyMatch(c -> c.name.equals(f.name) && c.partOfKey))
                                .collect(Collectors.toList());

                        updatedFields = toFields(update.columnsAfterUpdate, relation)
                                .stream()
                                .filter(f -> relation.columns.stream().filter(c -> c.name.equals(f.name))
                                        .findAny().map(c -> !c.partOfKey).orElse(true)
                                )
                                .collect(Collectors.toList());
                    }

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

    private List<Field<?>> toFieldsV2(List<TupleDataColumn> columns, RelationMessage relation, Optional<List<Integer>> filterColumnOrdinals) {
        if (columns.size() > relation.columns.size()) {
            throw new IllegalArgumentException(format("Tuple data columns length (%d) must be equal or less than relation columns (%d)", columns.size(), relation.columns.size()));
        }

        List<Field<?>> fields = new ArrayList<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            if (filterColumnOrdinals.isPresent() && !filterColumnOrdinals.get().contains(i + 1)) {
                continue;
            }

            TupleDataColumn insertCol = columns.get(i);
            RelationMessage.Column relationCol = relation.columns.get(i);
            Field.Type type = pgDataTypes.get(relationCol.dataOid).getType();

            fields.add(mapTupleDataToField(relationCol.name, type, insertCol));
        }

        return fields;
    }

    private void removeNextTransactions(Connection connection, String upToLsn) {
        String sql = "SELECT 1 FROM pg_logical_slot_get_binary_changes(?, ?, NULL, 'messages', 'true', 'proto_version', '1', 'publication_names', ?)";

        query(connection, sql, statement -> {
            var obj = new PGobject();
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
