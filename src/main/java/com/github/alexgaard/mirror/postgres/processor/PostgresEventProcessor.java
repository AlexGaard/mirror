package com.github.alexgaard.mirror.postgres.processor;

import com.github.alexgaard.mirror.core.EventProcessor;
import com.github.alexgaard.mirror.core.event.*;
import com.github.alexgaard.mirror.postgres.event.DeleteEvent;
import com.github.alexgaard.mirror.postgres.event.Field;
import com.github.alexgaard.mirror.postgres.event.InsertEvent;
import com.github.alexgaard.mirror.postgres.utils.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;
import static com.github.alexgaard.mirror.postgres.utils.SqlFieldType.sqlFieldType;
import static java.lang.String.format;

public class PostgresEventProcessor implements EventProcessor {

    private final static Logger log = LoggerFactory.getLogger(PostgresEventProcessor.class);

    private final AtomicBoolean originalAutoCommit = new AtomicBoolean();

    private final DataSource dataSource;

    public PostgresEventProcessor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public synchronized void process(EventTransaction transaction) {
        // TODO: Keep track of xid, skip if xid is older

        try (Connection connection = dataSource.getConnection()) {
            originalAutoCommit.set(connection.getAutoCommit());
            connection.setAutoCommit(false);

            try {
                transaction.events.forEach(e -> handleDataChangeEvent(e, connection));
            } catch (Exception e) {
                log.error("Caught exception while processing events", e);
                connection.rollback();
                connection.setAutoCommit(originalAutoCommit.get());
                throw softenException(e);
            }

            connection.commit();
            connection.setAutoCommit(originalAutoCommit.get());
        } catch (SQLException e) {
            throw softenException(e);
        }
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
        String whereSql = delete.identifyingFields.stream().map(f -> f.name + " = ?")
                .collect(Collectors.joining(", "));

        String sql = format("DELETE FROM %s.%s WHERE %s", delete.namespace, delete.table, whereSql);

        QueryUtils.update(connection, sql, statement -> {
            for (int i = 0; i < delete.identifyingFields.size(); i++) {
                Field field = delete.identifyingFields.get(i);

                setParameter(statement, i + 1, field);
            }

            statement.executeUpdate();
        });
    }

    private static String createSqlFieldsParameters(List<Field> fields) {
        return fields.stream().map(f -> f.name).collect(Collectors.joining(","));
    }

    private static String createSqlValueTemplateParameters(List<Field> fields) {
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

    private static void setParameter(PreparedStatement statement, int parameterIdx, Field field) throws SQLException {
        switch (field.type) {
            case BYTES:
                statement.setBytes(parameterIdx, (byte[]) field.value);
                break;
            default:
                int sqlType = sqlFieldType(field.type);
                statement.setObject(parameterIdx, field.value, sqlType);
        }
    }

}
