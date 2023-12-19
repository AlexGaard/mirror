package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.core.Processor;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.event.Event;
import com.github.alexgaard.mirror.core.event.EventTransaction;
import com.github.alexgaard.mirror.postgres.event.DeleteEvent;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.event.PostgresEvent;
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

public class PostgresProcessor implements Processor {

    private final static Logger log = LoggerFactory.getLogger(PostgresProcessor.class);

    private final AtomicBoolean originalAutoCommit = new AtomicBoolean();

    private final Map<String, Integer> lastSourceTransactionId = new HashMap<>();

    private final DataSource dataSource;

    public PostgresProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public synchronized Result process(EventTransaction transaction) {
        int lastTransactionId = lastSourceTransactionId.getOrDefault(transaction.sourceName, 0);

        List<Event> filteredEvents = filterNewEvents(transaction.events, lastTransactionId);

        if (filteredEvents.isEmpty()) {
            return Result.ok();
        }

        try (Connection connection = dataSource.getConnection()) {
            originalAutoCommit.set(connection.getAutoCommit());
            connection.setAutoCommit(false);

            try {
                filteredEvents.forEach(e -> handleDataChangeEvent(e, connection));

                insertSkipTransactionMessage(connection);

                lastSourceTransactionId.put(transaction.sourceName, findLastTransactionId(filteredEvents));
            } catch (Exception e) {
                log.error("Caught exception while processing events", e);
                connection.rollback();
                connection.setAutoCommit(originalAutoCommit.get());
                return Result.error(e);
            }

            connection.commit();
            connection.setAutoCommit(originalAutoCommit.get());
        } catch (SQLException e) {
            return Result.error(e);
        }

        return Result.ok();
    }

    private void handleDataChangeEvent(Event event, Connection connection) {
        if (event instanceof InsertEvent) {
            handleInsertDataChange((InsertEvent) event, connection);
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

    private void handleDeleteEvent(DeleteEvent delete, Connection connection) {
        String whereSql = delete.identifierFields.stream().map(f -> f.name + " = ?")
                .collect(Collectors.joining(", "));

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

    private static List<Event> filterNewEvents(List<Event> events, int lastTransactionId) {
        return events.stream().filter(e -> {
            if (e instanceof PostgresEvent) {
                var pgEvent = (PostgresEvent) e;

                boolean isNew = pgEvent.transactionId > lastTransactionId;

                if (!isNew) {
                    log.warn("Skipping event with old transaction id {}. Last transaction id was {}", pgEvent.transactionId, lastTransactionId);
                }

                return isNew;
            }

            return true;
        }).collect(Collectors.toList());
    }

    private static Integer findLastTransactionId(List<Event> events) {
        for (int i = events.size() - 1; i >= 0; i--) {
            Event event = events.get(i);

            if (event instanceof PostgresEvent) {
                return ((PostgresEvent) event).transactionId;
            }
        }

        return 0;
    }

}
