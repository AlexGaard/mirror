package com.github.alexgaard.mirror.postgres.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public class CustomWalMessage {

    public static final String MESSAGE_PREFIX = "mirror";

    public static final String SKIP_TRANSACTION_MSG = "skip-transaction";

    public static void insertSkipTransactionMessage(Connection connection) {
        insertCustomMessage(connection, MESSAGE_PREFIX, SKIP_TRANSACTION_MSG);
    }

    public static void insertCustomMessage(Connection connection, String name, String data) {
        String sql = "SELECT 1 FROM pg_logical_emit_message(true, ?, ?)";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.setString(2, data);
            statement.executeQuery();
        } catch (Exception e) {
            throw softenException(e);
        }
    }

}
