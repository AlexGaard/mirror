package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.Collector;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.event.Event;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.core.event.EventTransactionConsumer;
import com.github.alexgaard.mirror.postgres.collector.message.*;
import com.github.alexgaard.mirror.postgres.event.DeleteEvent;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.event.UpdateEvent;
import com.github.alexgaard.mirror.postgres.utils.PgMetadata;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.runWithResult;
import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.safeRunnable;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.MESSAGE_PREFIX;
import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.SKIP_TRANSACTION_MSG;
import static com.github.alexgaard.mirror.postgres.utils.FieldMapper.mapTupleDataToField;
import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
import static java.lang.String.format;

public class PostgresCollector implements Collector {

    private final Logger log = LoggerFactory.getLogger(PostgresCollector.class);

    private final String replicationSlotName;

    private final String publicationName;

    private final int maxChangesPrPoll = 500;

    private final long backoffIncreaseMs = 1000;

    private final long maxBackoffMs = 10_000;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final String sourceName;

    private final Map<Integer, PgMetadata.PgDataType> pgDataTypes = new HashMap<>();

    private final DataSource dataSource;

    private final Duration dataChangePollInterval;

    private final PgReplication pgReplication;

    private final AtomicInteger lastWalMessageCount = new AtomicInteger(0);

    private EventTransactionConsumer onTransactionCollected;

    private volatile boolean isStarted = false;

    private long currentBackoffMs = 0;

    private ScheduledFuture<?> pollSchedule;

    public PostgresCollector(String sourceName, DataSource dataSource, Duration pollInterval, PgReplication pgReplication) {
        this.sourceName = sourceName;
        this.dataSource = dataSource;
        this.dataChangePollInterval = pollInterval;
        this.pgReplication = pgReplication;
        this.replicationSlotName = pgReplication.getReplicationSlotName();
        this.publicationName = pgReplication.getPublicationName();
    }

    public PostgresCollector(String sourceName, DataSource dataSource, PgReplication pgReplication) {
        this(sourceName, dataSource, Duration.ofSeconds(1), pgReplication);
    }

    @Override
    public void setOnTransactionCollected(EventTransactionConsumer onTransactionCollected) {
        if (onTransactionCollected == null) {
            throw new IllegalArgumentException("onTransactionCollected cannot be null");
        }

        this.onTransactionCollected = onTransactionCollected;
    }

    @Override
    public synchronized void start() {
        if (isStarted) {
            return;
        }

        if (onTransactionCollected == null) {
            throw new IllegalStateException("onTransactionCollected is missing");
        }

        isStarted = true;

        pgDataTypes.putAll(PgMetadata.getAllPgDataTypes(dataSource));

        pgReplication.setup(dataSource);

        pollSchedule = executorService.scheduleWithFixedDelay(
                safeRunnable(this::checkForDataChanges),
                0,
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

    public int getLastWalMessageCount() {
        return lastWalMessageCount.get();
    }

    private void checkForDataChanges() {
        try (Connection connection = dataSource.getConnection()) {
            List<RawMessage> rawMessages = peekDataChanges(connection);

            lastWalMessageCount.set(rawMessages.size());

            if (rawMessages.isEmpty()) {
                return;
            }

            List<Message> messages = rawMessages
                    .stream()
                    .map(MessageParser::parse)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            List<List<Message>> transactions = splitIntoTransactions(messages);

            for (int i = 0; i < transactions.size(); i++) {
                List<Message> transactionMessages = transactions.get(i);

                if (shouldTransactionBeSkipped(transactionMessages)) {
                    continue;
                }

                List<Event> transactionEvents = toEventTransaction(messages);

                CommitMessage commit = (CommitMessage) transactionMessages.stream()
                        .filter(m -> m.type.equals(Message.Type.COMMIT))
                        .findAny()
                        .orElseThrow(() -> new IllegalStateException("Commit message is missing from transaction"));

                var transaction = new EventTransaction(
                        UUID.randomUUID(),
                        sourceName,
                        transactionEvents,
                        OffsetDateTime.now(), //TODO: pgTimestampToEpochMs(commit.commitTimestamp),
                        OffsetDateTime.now()
                );

                Result result = runWithResult(() -> onTransactionCollected.consume(transaction));

                if (result.isError()) {
                    removeNextTransactions(connection, i + 1);

                    log.error("Caught exception while sending events", result.getError().get());

                    currentBackoffMs = Math.min(currentBackoffMs + backoffIncreaseMs, maxBackoffMs);
                    Thread.sleep(currentBackoffMs);
                    return;
                }
            }

            removeNextTransactions(connection, transactions.size());
            currentBackoffMs = 0;
        } catch (Exception e) {
            log.error("Caught exception while retrieving WAL messages", e);
        }
    }

    private List<Event> toEventTransaction(List<Message> msgTransaction) {
        List<Event> eventTransaction = new ArrayList<>();

        for (int i = 0; i < msgTransaction.size(); i++) {
            Message message = msgTransaction.get(i);

            switch (message.type) {
                case INSERT: {
                    InsertMessage insert = (InsertMessage) message;

                    RelationMessage relation = (RelationMessage) msgTransaction.stream()
                            .filter(t -> t instanceof RelationMessage && ((RelationMessage) t).oid == insert.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field<?>> fields = toFields(insert.columns, relation);

                    InsertEvent insertDataChange = new InsertEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            insert.xid,
                            fields,
                            OffsetDateTime.now()
                    );

                    eventTransaction.add(insertDataChange);
                    break;
                }
                case DELETE: {
                    DeleteMessage delete = (DeleteMessage) message;

                    RelationMessage relation = (RelationMessage) msgTransaction.stream()
                            .filter(t -> t instanceof RelationMessage && ((RelationMessage) t).oid == delete.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field<?>> fields = toFields(delete.columns, relation);

                    DeleteEvent deleteEvent = new DeleteEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            delete.xid,
                            fields,
                            OffsetDateTime.now()
                    );

                    eventTransaction.add(deleteEvent);
                    break;
                }
                case UPDATE: {
                    UpdateMessage update = (UpdateMessage) message;

                    RelationMessage relation = (RelationMessage) msgTransaction.stream()
                            .filter(t -> t instanceof RelationMessage && ((RelationMessage) t).oid == update.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    // TODO: Check relation for which field is part of key
                    // If type = K, then use identifyingColumns, else use relation with partOfKey

                    List<Field<?>> identifyingFields = toFields(update.identifyingColumns, relation);
                    List<Field<?>> updatedFields = toFields(update.updatedColumns, relation);

                    UpdateEvent updateEvent = new UpdateEvent(
                            UUID.randomUUID(),
                            relation.namespace,
                            relation.relationName,
                            update.xid,
                            identifyingFields,
                            updatedFields,
                            OffsetDateTime.now()
                    );

                    eventTransaction.add(updateEvent);
                    break;
                }
            }

        }

        return eventTransaction;
    }

    private List<Field<?>> toFields(List<TupleDataColumn> columns, RelationMessage relation) {
        if (columns.size() > relation.columns.size()) {
            throw new IllegalArgumentException(format("Tuple data columns length (%d) must be equal or less than relation columns (%d)", columns.size(), relation.columns.size()));
        }

        List<Field<?>> fields = new ArrayList<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            TupleDataColumn insertCol = columns.get(i);
            RelationMessage.Column relationCol = relation.columns.get(i);

            Field.Type type = insertCol.type.equals(TupleDataColumn.Type.NULL)
                    ? Field.Type.NULL
                    : pgDataTypes.get(relationCol.dataOid).getType();

            fields.add(mapTupleDataToField(relationCol.name, type, insertCol));
        }

        return fields;
    }

    private void removeNextTransactions(Connection connection, int transactionsToRemove) {
        String sql = "SELECT 1 FROM pg_logical_slot_get_binary_changes(?, NULL, ?, 'messages', 'true', 'proto_version', '1', 'publication_names', ?)";

        query(connection, sql, statement -> {
            statement.setString(1, replicationSlotName);
            statement.setInt(2, transactionsToRemove);
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

            return resultList(resultSet, PostgresCollector::toRawEvent);
        });
    }

    private static List<List<Message>> splitIntoTransactions(List<Message> messages) {
        return new ArrayList<>(messages.stream()
                .sorted(Comparator.comparingInt(m -> m.xid))
                .collect(Collectors.groupingBy((m -> m.xid)))
                .values());
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
