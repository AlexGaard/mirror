package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.core.EventSink;
import com.github.alexgaard.mirror.core.Result;
import com.github.alexgaard.mirror.core.Event;
import com.github.alexgaard.mirror.postgres.event.*;
import com.github.alexgaard.mirror.postgres.processor.config.InsertConflictStrategy;
import com.github.alexgaard.mirror.postgres.processor.config.ProcessorConfig;
import com.github.alexgaard.mirror.postgres.processor.config.ProcessorTableConfig;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.postgres.metadata.PgMetadata.tableFullName;
import static com.github.alexgaard.mirror.postgres.utils.CustomWalMessage.insertSkipTransactionMessage;
import static java.lang.String.format;

public class PostgresEventProcessor implements EventSink {

    private final static Logger log = LoggerFactory.getLogger(PostgresEventProcessor.class);

    private final AtomicBoolean originalAutoCommit = new AtomicBoolean();

    private final Map<String, Integer> lastSourceTransactionId = new HashMap<>();

    private final ProcessorConfig config;

    private final DataSource dataSource;

    public PostgresEventProcessor(DataSource dataSource) {
        this.config = new ProcessorConfig();
        this.dataSource = dataSource;
    }

    public PostgresEventProcessor(ProcessorConfig config, DataSource dataSource) {
        this.config = config;
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

                connection.commit();

                lastSourceTransactionId.put(transaction.sourceName, findLastTransactionId(filteredEvents));

                return Result.ok();
            } catch (Exception e) {
                log.error("Caught exception while processing events", e);
                connection.rollback();
                return Result.error(e);
            } finally {
                connection.setAutoCommit(originalAutoCommit.get());
            }
        } catch (Exception e) {
            return Result.error(e);
        }
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
        String fields = createSqlFieldParameters(insert.fields);
        String templateParams = createSqlValuesTemplate(insert.fields);
        String onConflictSql = createOnConflictSql(insert, config.getTableConfig().get(tableFullName(insert.namespace, insert.table)));

        String sql = format("INSERT INTO %s.%s (%s) VALUES (%s) %s", insert.namespace, insert.table, fields, templateParams, onConflictSql);

        QueryUtils.update(connection, sql, statement -> {
            int paramCounter = 1;

            for (Field<?> field : insert.fields) {
                setParameter(connection, statement, paramCounter++, field);
            }

            statement.executeUpdate();
        });
    }

    private void handleUpdateEvent(UpdateEvent update, Connection connection) {
        String setSql = createSqlSetAllFields(update.fields);

        String whereSql = createSqlWhereAllFieldsEqualTemplate(update.identifierFields);

        String sql = format("UPDATE %s.%s SET %s WHERE %s", update.namespace, update.table, setSql, whereSql);

        QueryUtils.update(connection, sql, statement -> {
            int paramCounter = 1;

            for (Field<?> field : update.fields) {
                setParameter(connection, statement, paramCounter++, field);
            }

            for (Field<?> field : update.identifierFields) {
                // Fields that have null use "is null" and does not have a template parameter
                if (field.value != null) {
                    setParameter(connection, statement, paramCounter++, field);
                }
            }

            statement.executeUpdate();
        });
    }

    private void handleDeleteEvent(DeleteEvent delete, Connection connection) {
        String whereSql = createSqlWhereAllFieldsEqualTemplate(delete.identifierFields);

        String sql = format("DELETE FROM %s.%s WHERE %s", delete.namespace, delete.table, whereSql);

        QueryUtils.update(connection, sql, statement -> {
            int paramCounter = 1;

            for (Field<?> field : delete.identifierFields) {
                // Fields that have null use "is null" and does not have a template parameter
                if (field.value != null) {
                    setParameter(connection, statement, paramCounter++, field);
                }
            }

            statement.executeUpdate();
        });
    }

    private static String createOnConflictSql(InsertEvent insert, ProcessorTableConfig config) {
        if (config == null) {
            return "";
        }

        if (config.insertConflictConstraint == null) {
            throw new IllegalArgumentException("Config is missing insertConflictIndex");
        }

        if (config.insertConflictStrategy == InsertConflictStrategy.DO_NOTHING) {
            return format("ON CONFLICT ON CONSTRAINT %s DO NOTHING", config.insertConflictConstraint);
        } else {
            String setFieldsSql = insert.fields.stream()
                    .map(f -> format("%s = EXCLUDED.%s", f.name, f.name))
                    .collect(Collectors.joining(", "));

            return format("ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s", config.insertConflictConstraint, setFieldsSql);
        }
    }

    private static String createSqlFieldParameters(List<Field<?>> fields) {
        return fields.stream().map(f -> f.name).collect(Collectors.joining(","));
    }

    private static String createSqlValuesTemplate(List<Field<?>> fields) {
        return fields.stream()
                .map(f -> "?" + postgresTypeCast(f))
                .collect(Collectors.joining(", "));
    }

    private static String createSqlWhereAllFieldsEqualTemplate(List<Field<?>> fields) {
        return fields.stream()
                .map(f -> {
                    if (f.value == null) {
                        return f.name + " is null";
                    }

                    String fieldCast;
                    String valueCast;

                    if (FieldType.JSON.equals(f.type) || FieldType.JSONB.equals(f.type)) {
                        fieldCast = "::jsonb";
                        valueCast = "::jsonb";
                    } else {
                        fieldCast = "";
                        valueCast = postgresTypeCast(f);
                    }

                    return f.name + fieldCast + " = ?" + valueCast;
                })
                .collect(Collectors.joining(" and "));
    }

    private static String createSqlSetAllFields(List<Field<?>> fields) {
        return fields.stream().map(f -> f.name + " = ?" + postgresTypeCast(f))
                .collect(Collectors.joining(", "));
    }

    private static String postgresTypeCast(Field<?> field) {
        switch (field.type) {
            case JSON:
                return "::json";
            case JSONB:
                return "::jsonb";
            case UUID:
                return "::uuid";
            default:
                return "";
        }
    }

    private static void setParameter(Connection connection, PreparedStatement statement, int parameterIdx, Field<?> field) throws SQLException {
        if (FieldType.BYTES.equals(field.type)) {
            statement.setBytes(parameterIdx, (byte[]) field.value);
        } else {
            int sqlType = field.toSqlFieldType();

            if (sqlType == Types.ARRAY) {
                Array array = connection.createArrayOf(field.type.toBasePgType(), ((List<?>) field.value).toArray());
                statement.setArray(parameterIdx, array);
            } else {
                statement.setObject(parameterIdx, field.value, sqlType);
            }
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
