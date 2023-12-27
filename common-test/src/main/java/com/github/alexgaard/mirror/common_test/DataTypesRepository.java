package com.github.alexgaard.mirror.common_test;


import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static com.github.alexgaard.mirror.common_test.QueryUtils.resultList;
import static com.github.alexgaard.mirror.common_test.QueryUtils.update;

public class DataTypesRepository {

    private final DataSource dataSource;

    public DataTypesRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    public Optional<DataTypesDbo> getDataTypes(int id) {
        return QueryUtils.query(dataSource, "SELECT * FROM data_types WHERE id = ?", statement -> {
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();

            return resultList(resultSet, DataTypesRepository::mapRowToDataTypesDbo)
                    .stream()
                    .findAny();
        });
    }

    public int getDataTypesCount() {
        return QueryUtils.query(dataSource, "SELECT count(*) FROM data_types", statement -> {
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            return resultSet.getInt(1);
        });
    }

    public void deleteDataTypeRow(int id) {
        String sql = "DELETE FROM data_types WHERE id = ?";

        update(dataSource, sql, statement -> {
            statement.setInt(1, id);
            statement.executeUpdate();
        });
    }

    public void clear() {
        update(dataSource, "DELETE FROM data_types");
    }

    public void insertDataTypes(DataTypesDbo dbo) {
        String sql = "insert into data_types (" +
                "int2_field," +
                "int4_field," +
                "int8_field," +
                "float4_field," +
                "float8_field," +
                "uuid_field," +
                "varchar_field," +
                "text_field," +
                "bool_field," +
                "bytes_field," +
                "char_field," +
                "json_field," +
                "jsonb_field," +
                "date_field," +
                "time_field," +
                "timestamp_field," +
                "timestamptz_field," +
                "id"+
                ") values (?,?,?,?,?,?::uuid,?,?,?,?,?,?::json,?::jsonb,?,?,?,?,?)";

        update(dataSource, sql, statement -> {
            setFields(dbo, statement, 0);

            if (dbo.id != null) {
                statement.setInt(18, dbo.id);
            } else {
                statement.setNull(18, Types.INTEGER);
            }

            statement.executeUpdate();
        });
    }

    public void updateDataTypes(DataTypesDbo dbo) {
        String sql = "update data_types set " +
                "int2_field = ?," +
                "int4_field = ?," +
                "int8_field = ?," +
                "float4_field = ?," +
                "float8_field = ?," +
                "uuid_field = ?::uuid," +
                "varchar_field = ?," +
                "text_field = ?," +
                "bool_field = ?," +
                "bytes_field = ?," +
                "char_field = ?," +
                "json_field = ?::json," +
                "jsonb_field = ?::jsonb," +
                "date_field = ?," +
                "time_field = ?," +
                "timestamp_field = ?," +
                "timestamptz_field = ?" +
                " where id = ?";

        update(dataSource, sql, statement -> {
            setFields(dbo, statement, 0);
            statement.setInt(18, dbo.id);
            statement.executeUpdate();
        });
    }

    private static void setFields(DataTypesDbo dbo, PreparedStatement statement, int startIdx) throws SQLException {
        if (dbo.int2_field != null) {
            statement.setShort(startIdx + 1, dbo.int2_field);
        } else {
            statement.setNull(startIdx + 1, Types.SMALLINT);
        }

        if (dbo.int4_field != null) {
            statement.setInt(startIdx + 2, dbo.int4_field);
        } else {
            statement.setNull(startIdx + 2, Types.INTEGER);
        }

        if (dbo.int8_field != null) {
            statement.setLong(startIdx + 3, dbo.int8_field);
        } else {
            statement.setNull(startIdx + 3, Types.BIGINT);
        }

        if (dbo.float4_field != null) {
            statement.setFloat(startIdx + 4, dbo.float4_field);
        } else {
            statement.setNull(startIdx + 4, Types.FLOAT);
        }

        if (dbo.float8_field != null) {
            statement.setDouble(startIdx + 5, dbo.float8_field);
        } else {
            statement.setNull(startIdx + 5, Types.DOUBLE);
        }

        if (dbo.uuid_field != null) {
            statement.setString(startIdx + 6, dbo.uuid_field.toString());
        } else {
            statement.setNull(startIdx + 6, Types.VARCHAR);
        }

        if (dbo.varchar_field != null) {
            statement.setString(startIdx + 7, dbo.varchar_field);
        } else {
            statement.setNull(startIdx + 7, Types.VARCHAR);
        }

        if (dbo.text_field != null) {
            statement.setString(startIdx + 8, dbo.text_field);
        } else {
            statement.setNull(startIdx + 8, Types.VARCHAR);
        }

        if (dbo.bool_field != null) {
            statement.setBoolean(startIdx + 9, dbo.bool_field);
        } else {
            statement.setNull(startIdx + 9, Types.BOOLEAN);
        }

        if (dbo.bytes_field != null) {
            statement.setBytes(startIdx + 10, dbo.bytes_field);
        } else {
            statement.setNull(startIdx + 10, Types.BINARY);
        }

        if (dbo.char_field != null) {
            statement.setString(startIdx + 11, dbo.char_field.toString());
        } else {
            statement.setNull(startIdx + 11, Types.CHAR);
        }

        if (dbo.json_field != null) {
            statement.setString(startIdx + 12, dbo.json_field);
        } else {
            statement.setNull(startIdx + 12, Types.VARCHAR);
        }

        if (dbo.jsonb_field != null) {
            statement.setString(startIdx + 13, dbo.jsonb_field);
        } else {
            statement.setNull(startIdx + 13, Types.VARCHAR);
        }

        if (dbo.date_field != null) {
            statement.setObject(startIdx + 14, dbo.date_field);
        } else {
            statement.setNull(startIdx + 14, Types.DATE);
        }

        if (dbo.time_field != null) {
            statement.setObject(startIdx + 15, dbo.time_field);
        } else {
            statement.setNull(startIdx + 15, Types.TIME);
        }

        if (dbo.timestamp_field != null) {
            statement.setObject(startIdx + 16, dbo.timestamp_field);
        } else {
            statement.setNull(startIdx + 16, Types.TIMESTAMP);
        }

        if (dbo.timestamptz_field != null) {
            statement.setObject(startIdx + 17, dbo.timestamptz_field);
        } else {
            statement.setNull(startIdx + 17, Types.TIME_WITH_TIMEZONE);
        }
    }

    private static DataTypesDbo mapRowToDataTypesDbo(ResultSet rs) throws SQLException {
        DataTypesDbo dbo = new DataTypesDbo();
        dbo.id = rs.getInt("id");
        dbo.int2_field = rs.getObject("int2_field", Short.class);
        dbo.int4_field = rs.getObject("int4_field", Integer.class);
        dbo.int8_field = rs.getObject("int8_field", Long.class);
        dbo.float4_field = rs.getObject("float4_field", Float.class);
        dbo.float8_field = rs.getObject("float8_field", Double.class);
        dbo.uuid_field = uuid(rs.getString("uuid_field"));
        dbo.varchar_field = rs.getString("varchar_field");
        dbo.text_field = rs.getString("text_field");
        dbo.bool_field = rs.getObject("bool_field", Boolean.class);
        dbo.bytes_field = rs.getBytes("bytes_field");
        dbo.char_field = character(rs.getString("char_field"));
        dbo.json_field = rs.getString("json_field");
        dbo.jsonb_field = rs.getString("jsonb_field");
        dbo.date_field = rs.getObject("date_field", LocalDate.class);
        dbo.time_field = rs.getObject("time_field", LocalTime.class);
        dbo.timestamp_field = rs.getObject("timestamp_field", LocalDateTime.class);
        dbo.timestamptz_field = rs.getObject("timestamptz_field", OffsetDateTime.class);
        return dbo;
    }

    private static UUID uuid(String str) {
        return str != null ? UUID.fromString(str) : null;
    }

    private static Character character(String str) {
        return str != null ? str.charAt(0) : null;
    }

}
