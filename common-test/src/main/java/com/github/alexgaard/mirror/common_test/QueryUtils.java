package com.github.alexgaard.mirror.common_test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.github.alexgaard.mirror.core.utils.ExceptionUtil.softenException;

public class QueryUtils {

    public interface ResultSetMapper<T> {
        T map(ResultSet resultSet) throws SQLException;

    }

    public interface PreparedStatementQuery<T> {
        T execute(PreparedStatement statement) throws SQLException;

    }

    public interface PreparedStatementQueryWithoutResult {
        void execute(PreparedStatement statement) throws SQLException;

    }

    public static <T> List<T> resultList(ResultSet resultSet, ResultSetMapper<T> mapper) throws SQLException {
        List<T> events = new ArrayList<>();

        while (resultSet.next()) {
            events.add(mapper.map(resultSet));
        }

        return events;
    }

    public static <T> T query(DataSource dataSource, String sql, PreparedStatementQuery<T> query) {
        try (Connection connection = dataSource.getConnection()) {
            return query(connection, sql, query);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static void query(DataSource dataSource, String sql, PreparedStatementQueryWithoutResult query) {
        try (Connection connection = dataSource.getConnection()) {
            query(connection, sql, query);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static <T> T query(Connection connection, String sql, PreparedStatementQuery<T> query) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            return query.execute(statement);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static void query(Connection connection, String sql, PreparedStatementQueryWithoutResult query) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            query.execute(statement);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static int update(DataSource dataSource, String sql, PreparedStatementQuery<Integer> query) {
        try (Connection connection = dataSource.getConnection()) {
            return update(connection, sql, query);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static void update(DataSource dataSource, String sql, PreparedStatementQueryWithoutResult query) {
        try (Connection connection = dataSource.getConnection()) {
            update(connection, sql, query);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static int update(DataSource dataSource, String sql) {
        try (Connection connection = dataSource.getConnection()) {
            return update(connection, sql);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static int update(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static int update(Connection connection, String sql, PreparedStatementQuery<Integer> query) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            return query.execute(statement);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

    public static void update(Connection connection, String sql, PreparedStatementQueryWithoutResult query) {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            query.execute(statement);
        } catch (SQLException e) {
            throw softenException(e);
        }
    }

}
