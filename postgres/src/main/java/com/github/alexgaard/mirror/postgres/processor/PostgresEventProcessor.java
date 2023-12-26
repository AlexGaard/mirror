package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.core.EventSink;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.*;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.postgres.utils.CustomMessage.insertSkipTransactionMessage;
import static com.github.alexgaard.mirror.postgres.utils.SqlFieldType.sqlFieldType;
import static java.lang.String.format;

public class PostgresEventProcessor implements EventSink {

    private final static Logger log = LoggerFactory.getLogger(PostgresEventProcessor.class);

    private final AtomicBoolean originalAutoCommit = new AtomicBoolean();

    private final Map<String, Integer> lastSourceTransactionId = new HashMap<>();

    private final DataSource dataSource;

    public PostgresEventProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public synchronized Result consume(Event event) {
        if (!(event instanceof PostgresTransactionEvent)) {
            return Result.ok();
        }

        PostgresTransactionEvent transaction = (PostgresTransactionEvent) event;

        int lastTransactionId = lastSourceTransactionId.getOrDefault(transaction.sourceName, 0);

        List<DataChangeEvent> filteredEvents = filterNewEvents(transaction.events, lastTransactionId);

        if (filteredEvents.isEmpty()) {
            return Result.ok();
        }

        try (Connection connection = dataSource.getConnection()) {
            originalAutoCommit.set(connection.getAutoCommit());
            connection.setAutoCommit(false);

            try {
                filteredEvents.forEach(e -> handleDataChangeEvent(e, connection));

                insertSkipTransactionMessage(connection);
            } catch (Exception e) {
                log.error("Caught exception while processing events", e);
                connection.rollback();
                connection.setAutoCommit(originalAutoCommit.get());
                return Result.error(e);
            }

            connection.commit();
            connection.setAutoCommit(originalAutoCommit.get());

            lastSourceTransactionId.put(transaction.sourceName, findLastTransactionId(filteredEvents));
        } catch (Exception e) {
            return Result.error(e);
        }

        return Result.ok();
    }

    private void handleDataChangeEvent(DataChangeEvent event, Connection connection) {
        if (event instanceof InsertEvent) {
            handleInsertDataChange((InsertEvent) event, connection);
        } else if (event instanceof UpdateEvent) {
            handleUpdateEvent((UpdateEvent) event, connection);
        } else if (event instanceof DeleteEvent) {
            handleDeleteEvent((DeleteEvent) event, connection);
        }
    }

    private void handleInsertDataChange(InsertEvent insert, Connection connection) {
        // Add option for on conflict do nothing
        // Add field validation

        String fields = createSqlFieldsParameters(insert.fields);

        String templateParams = createSqlValueTemplateParameters(insert.fields);

        String sql = format("INSERT INTO %s.%s (%s) VALUES (%s)", insert.namespace, insert.table, fields, templateParams);

        QueryUtils.update(connection, sql, statement -> {
            for (int i = 0; i < insert.fields.size(); i++) {
                Field field = insert.fields.get(i);

                setParameter(statement, i + 1, field);
            }

            statement.executeUpdate();
        });
    }

    private void handleUpdateEvent(UpdateEvent update, Connection connection) {
        String setSql = update.fields.stream().map(f -> f.name + " = ?")
                .collect(Collectors.joining(", "));

        String whereSql = update.identifierFields.stream().map(f -> f.name + " = ?")
                .collect(Collectors.joining(" and "));

        String sql = format("UPDATE %s.%s SET %s WHERE %s", update.namespace, update.table, setSql, whereSql);

        QueryUtils.update(connection, sql, statement -> {
            int paramCounter = 1;

            for (Field<?> field : update.fields) {
                setParameter(statement, paramCounter++, field);
            }

            for (Field<?> field : update.identifierFields) {
                setParameter(statement, paramCounter++, field);
            }

            statement.executeUpdate();
        });
    }

    private void handleDeleteEvent(DeleteEvent delete, Connection connection) {
        String whereSql = delete.identifierFields.stream().map(f -> f.name + " = ?")
                .collect(Collectors.joining(" and "));

        String sql = format("DELETE FROM %s.%s WHERE %s", delete.namespace, delete.table, whereSql);

        QueryUtils.update(connection, sql, statement -> {
            for (int i = 0; i < delete.identifierFields.size(); i++) {
                Field field = delete.identifierFields.get(i);

                setParameter(statement, i + 1, field);
            }

            statement.executeUpdate();
        });
    }

    private static String createSqlFieldsParameters(List<Field<?>> fields) {
        return fields.stream().map(f -> f.name).collect(Collectors.joining(","));
    }

    private static String createSqlValueTemplateParameters(List<Field<?>> fields) {
        return fields.stream()
                .map(f -> "?" + postgresTypeCast(f.type))
                .collect(Collectors.joining(","));
    }

    private static String postgresTypeCast(Field.Type type) {
        switch (type) {
            case JSON:
                return "::json";
            case UUID:
                return "::uuid";
            default:
                return "";
        }
    }

    private static void setParameter(PreparedStatement statement, int parameterIdx, Field<?> field) throws SQLException {
        switch (field.type) {
            case BYTES:
                statement.setBytes(parameterIdx, (byte[]) field.value);
                break;
            default:
                int sqlType = sqlFieldType(field.type);
                statement.setObject(parameterIdx, field.value, sqlType);
        }
    }

    private static List<DataChangeEvent> filterNewEvents(List<DataChangeEvent> events, int lastTransactionId) {
        return events.stream().filter(e -> {
            boolean isNew = e.transactionId > lastTransactionId;

            if (!isNew) {
                log.warn("Skipping event with old transaction id {}. Last transaction id was {}", e.transactionId, lastTransactionId);
            }

            return isNew;
        }).collect(Collectors.toList());
    }

    private static Integer findLastTransactionId(List<DataChangeEvent> events) {
        if (events.isEmpty()) {
            return 0;
        }

        return events.get(events.size() - 1).transactionId;
    }

}
