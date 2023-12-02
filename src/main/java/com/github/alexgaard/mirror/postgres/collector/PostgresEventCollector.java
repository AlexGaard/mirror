package com.github.alexgaard.mirror.postgres.collector;

import com.github.alexgaard.mirror.core.event.*;
import com.github.alexgaard.mirror.core.EventCollector;
import com.github.alexgaard.mirror.postgres.collector.message.*;
import com.github.alexgaard.mirror.postgres.utils.PgMetadata;
import com.github.alexgaard.mirror.postgres.utils.TupleDataColumn;
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
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.postgres.utils.QueryUtils.*;
import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.safeRunnable;
import static com.github.alexgaard.mirror.postgres.utils.PgFieldParser.parseFieldData;
import static java.lang.String.format;

public class PostgresEventCollector implements EventCollector {

    private final Logger log = LoggerFactory.getLogger(PostgresEventCollector.class);

    private final String replicationSlotName = "mirror";

    private final String publicationName = "mirror";

    private final String sourceName;

    private final Map<Integer, PgMetadata.PgDataType> pgDataTypes = new HashMap<>();

    private final int maxChangesPrPoll = 100;

    private final DataSource dataSource;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final Duration dataChangePollInterval;

    private final PgReplication pgReplication;

    private EventTransactionConsumer onDataChanged;

    private volatile boolean isStarted = false;

    private ScheduledFuture<?> pollSchedule;

    public PostgresEventCollector(String sourceName, DataSource dataSource, Duration pollInterval, PgReplication pgReplication) {
        this.sourceName = sourceName;
        this.dataSource = dataSource;
        this.dataChangePollInterval = pollInterval;
        this.pgReplication = pgReplication;
    }

    public PostgresEventCollector(String sourceName, DataSource dataSource, PgReplication pgReplication) {
        this(sourceName, dataSource, Duration.ofSeconds(1), pgReplication);
    }

    @Override
    public void initialize(EventTransactionConsumer onDataChanged) {
        this.onDataChanged = onDataChanged;
    }

    @Override
    public void start() {
        if (isStarted) {
            return;
        }

        if (onDataChanged == null) {
            throw new IllegalStateException("Must be initialized before start() can be used");
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
    public void stop() {
        if (!isStarted) {
            return;
        }

        isStarted = false;

        if (pollSchedule != null && !pollSchedule.isDone()) {
            pollSchedule.cancel(false);
        }
    }

    private void checkForDataChanges() {
        try (Connection connection = dataSource.getConnection()) {
            List<RawMessage> events = peekDataChanges(connection);

            List<List<Event>> transactions = parseChanges(events);

            for (int i = 0; i < transactions.size(); i++) {
                List<Event> transactionEvents = transactions.get(i);

                var transaction = new EventTransaction(
                        UUID.randomUUID(),
                        sourceName,
                        System.currentTimeMillis(), // TODO: get from BeginMessage
                        System.currentTimeMillis(),
                        transactionEvents
                );

                try {
                    onDataChanged.consume(transaction);
                } catch (Exception e) {
                    // TODO: add backoff
                    // Remove until lsn or nchanges
//                    removeNextDataChanges(connection, i);
                    log.error("Caught exception while sending events", e);
                    return;
                }
            }

//            removeNextDataChanges(connection, transactions.size());
        } catch (Exception e) {
            log.error("Caught exception while retrieving data changes", e);
        }
    }

    private List<List<Event>> parseChanges(List<RawMessage> rawMessages) {
        List<Message> messages = rawMessages
                .stream()
                .map(EventParser::parseEvent)
                .collect(Collectors.toList());


        return createTransactions(messages)
                .stream()
                .map(this::toDataChangeTransaction)
                .collect(Collectors.toList());
    }

    private List<Event> toDataChangeTransaction(List<Message> transaction) {
        List<Event> events = new ArrayList<>();

        for (int i = 0; i < transaction.size(); i++) {
            Message message = transaction.get(i);

            switch (message.type) {
                case INSERT:
                    InsertMessage insert = (InsertMessage) message;

                    RelationMessage relation = (RelationMessage) transaction.stream()
                            .filter(t -> t instanceof RelationMessage && ((RelationMessage) t).oid == insert.relationMessageOid)
                            .findAny()
                            .orElseThrow();

                    List<Field> fields = getFields(insert, relation);

                    InsertEvent insertDataChange = new InsertEvent(
                            UUID.randomUUID(),
                            System.currentTimeMillis(),
                            relation.namespace,
                            relation.relationName,
                            fields
                    );

                    events.add(insertDataChange);
                    break;
            }

        }

        return events;
    }

    private List<Field> getFields(InsertMessage insert, RelationMessage relation) {
        if (insert.columns.size() != relation.columns.size()) {
            throw new IllegalArgumentException(format("Insert columns had different length than relation columns: %d != %d", insert.columns.size(), relation.columns.size()));
        }

        List<Field> fields = new ArrayList<>(insert.columns.size());

        for (int i = 0; i < insert.columns.size(); i++) {
            TupleDataColumn insertCol = insert.columns.get(i);
            RelationMessage.Column relationCol = relation.columns.get(i);

            Object fieldData = insertCol.getData();
            Field.Type type = pgDataTypes.get(relationCol.dataOid).getType();

            Object parsedData = parseFieldData(type, fieldData);

            fields.add(new Field(relationCol.name, parsedData, type));
        }

        return fields;
    }

    private static List<List<Message>> createTransactions(List<Message> messages) {
        List<List<Message>> transactions = new ArrayList<>();

        List<Message> currentTransaction = null;

        // TODO: Check if we can insert skip messages into transaction with pg_logical_emit_message
        //  to prevent us from re-collecting message from other sources that we have just created

        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);

            switch (message.type) {
                case BEGIN:
                    currentTransaction = new ArrayList<>();
                    break;
                case RELATION:
                case INSERT:
                    if (currentTransaction == null) {
                        throw new IllegalStateException("No BEGIN message was found at the start of the transaction");
                    }

                    currentTransaction.add(message);
                    break;
                case COMMIT:
                    if (currentTransaction == null) {
                        throw new IllegalStateException("No BEGIN message was found at the start of the transaction");
                    }

                    transactions.add(currentTransaction);
                    break;
            }
        }

        return transactions;
    }

    private void removeNextDataChanges(Connection connection, int changesToRemove) {
        String sql = "SELECT 1 FROM pg_logical_slot_get_binary_changes(?, NULL, ?, 'proto_version', '1', 'publication_names', ?)";

        update(connection, sql, statement -> {
            statement.setString(1, replicationSlotName);
            statement.setInt(2, changesToRemove);
            statement.setString(3, publicationName);
            return statement.executeUpdate();
        });
    }

    private List<RawMessage> peekDataChanges(Connection connection) {
        String sql = "SELECT * FROM pg_logical_slot_peek_binary_changes(?, NULL, ?, 'proto_version', '1', 'publication_names', ?)";

        return query(connection, sql, (statement) -> {
            statement.setString(1, replicationSlotName);
            statement.setInt(2, maxChangesPrPoll);
            statement.setString(3, publicationName);

            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, PostgresEventCollector::toRawEvent);
        });
    }

    private static RawMessage toRawEvent(ResultSet resultSet) throws SQLException {
        String lsn = resultSet.getString("lsn");
        int xid = resultSet.getInt("xid");
        byte[] data = resultSet.getBytes("data");

        return new RawMessage(lsn, xid, data);
    }

}
