package com.github.alexgaard.mirror.common_test;


import javax.sql.DataSource;
import java.sql.*;
import java.time.*;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static com.github.alexgaard.mirror.common_test.QueryUtils.resultList;
import static com.github.alexgaard.mirror.common_test.QueryUtils.update;

public class DataArrayTypesRepository {

    private final DataSource dataSource;

    public DataArrayTypesRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    public Optional<DataArrayTypesDbo> getDataArrayTypes(int id) {
        return QueryUtils.query(dataSource, "SELECT * FROM data_array_types WHERE id = ?", statement -> {
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, DataArrayTypesRepository::mapRowToDataTypesDbo)
                    .stream()
                    .findAny();
        });
    }

    public void deleteDataTypeRow(int id) {
        String sql = "DELETE FROM data_array_types WHERE id = ?";

        update(dataSource, sql, statement -> {
            statement.setInt(1, id);
            statement.executeUpdate();
        });
    }

    public void clear() {
        update(dataSource, "DELETE FROM data_array_types");
    }

    public void insertDataTypes(DataArrayTypesDbo dbo) {
        String sql = "insert into data_array_types (" +
                "id," +
                "int2_array_field," +
                "int4_array_field," +
                "int8_array_field," +
                "float4_array_field," +
                "float8_array_field," +
                "uuid_array_field," +
                "varchar_array_field," +
                "text_array_field," +
                "bool_array_field," +
                "char_array_field," +
                "date_array_field," +
                "time_array_field," +
                "timestamp_array_field," +
                "timestamptz_array_field" +
                ") values (?,?,?,?,?,?,?::uuid[],?,?,?,?,?,?,?,?)";

        try (Connection connection = dataSource.getConnection()) {
            update(connection, sql, statement -> {
                statement.setInt(1, dbo.id);
                setFields(dbo, connection, statement, 1);
                statement.executeUpdate();
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void updateDataTypes(DataArrayTypesDbo dbo) {
        String sql = "update data_array_types set " +
                "int2_array_field = ?," +
                "int4_array_field = ?," +
                "int8_array_field = ?," +
                "float4_array_field = ?," +
                "float8_array_field = ?," +
                "uuid_array_field = ?::uuid[]," +
                "varchar_array_field = ?," +
                "text_array_field = ?," +
                "bool_array_field = ?," +
                "char_array_field = ?," +
                "date_array_field = ?," +
                "time_array_field = ?," +
                "timestamp_array_field = ?," +
                "timestamptz_array_field = ?" +
                " where id = ?";

        try (Connection connection = dataSource.getConnection()) {
            update(connection, sql, statement -> {
                int paramIdx = setFields(dbo, connection, statement, 0);
                statement.setInt(paramIdx, dbo.id);
                statement.executeUpdate();
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static int setFields(DataArrayTypesDbo dbo, Connection connection, PreparedStatement statement, int startIdx) throws SQLException {
        int paramIdx = startIdx + 1;

        if (dbo.int2_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("int2", dbo.int2_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.int4_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("int4", dbo.int4_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.int8_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("int8", dbo.int8_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.float4_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("float4", dbo.float4_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.float8_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("float8", dbo.float8_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.uuid_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("uuid", dbo.uuid_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.varchar_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("varchar", dbo.varchar_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.text_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("text", dbo.text_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.bool_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("bool", dbo.bool_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.char_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("char", dbo.char_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.date_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("date", dbo.date_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.time_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("time", dbo.time_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.timestamp_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("timestamp", dbo.timestamp_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        if (dbo.timestamptz_array_field != null) {
            statement.setArray(paramIdx++, connection.createArrayOf("timestamptz", dbo.timestamptz_array_field));
        } else {
            statement.setNull(paramIdx++, Types.ARRAY);
        }

        return paramIdx;
    }

    private static DataArrayTypesDbo mapRowToDataTypesDbo(ResultSet rs) throws SQLException {
        DataArrayTypesDbo dbo = new DataArrayTypesDbo();
        dbo.id = rs.getInt("id");
        dbo.int2_array_field = (Short[]) rs.getArray("int2_array_field").getArray();
        dbo.int4_array_field = (Integer[]) rs.getArray("int4_array_field").getArray();
        dbo.int8_array_field = (Long[]) rs.getArray("int8_array_field").getArray();
        dbo.float4_array_field = (Float[]) rs.getArray("float4_array_field").getArray();
        dbo.float8_array_field = (Double[]) rs.getArray("float8_array_field").getArray();
        dbo.uuid_array_field = (UUID[]) rs.getArray("uuid_array_field").getArray();
        dbo.varchar_array_field = (String[]) rs.getArray("varchar_array_field").getArray();
        dbo.text_array_field = (String[]) rs.getArray("text_array_field").getArray();
        dbo.bool_array_field = (Boolean[]) rs.getArray("bool_array_field").getArray();

        String[] charArray = (String[]) rs.getArray("char_array_field").getArray();
        dbo.char_array_field = fillArray(charArray, new Character[charArray.length], (str) -> str.charAt(0));

        Date[] dateArray = (Date[]) rs.getArray("date_array_field").getArray();
        dbo.date_array_field = fillArray(dateArray, new LocalDate[dateArray.length], Date::toLocalDate);

        Time[] timeArray = (Time[]) rs.getArray("time_array_field").getArray();
        dbo.time_array_field = fillArray(timeArray, new LocalTime[timeArray.length], Time::toLocalTime);

        Timestamp[] timestampArray = (Timestamp[]) rs.getArray("timestamp_array_field").getArray();
        dbo.timestamp_array_field = fillArray(timestampArray, new LocalDateTime[timestampArray.length], Timestamp::toLocalDateTime);

        Timestamp[] timestamptzArray = (Timestamp[]) rs.getArray("timestamptz_array_field").getArray();
        dbo.timestamptz_array_field = fillArray(timestamptzArray, new OffsetDateTime[timestamptzArray.length], (timestamp) -> OffsetDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.systemDefault()));

        return dbo;
    }

    private static <T, R> R[] fillArray(T[] srcArray, R[] destArray, Function<T, R> mapper) {
        for (int i = 0; i < srcArray.length; i++) {
            destArray[i] = mapper.apply(srcArray[i]);
        }

        return destArray;
    }

}
