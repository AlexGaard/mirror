package com.github.alexgaard.mirror.postgres.utils;

import java.util.ArrayList;
import java.util.List;

import static com.github.alexgaard.mirror.postgres.utils.ParseUtils.*;

public class PgoutputParser {

    private int pointer = 0;

    private final byte[] data;

    public PgoutputParser(byte[] data, int initialPointer) {
        this.data = data;
        pointer = initialPointer;
    }

    public PgoutputParser(byte[] data) {
        this.data = data;
    }

    public byte nextByte() {
        byte b = data[pointer];
        pointer += 1;
        return b;
    }

    public short nextShort() {
        short s = getShort(pointer, data);
        pointer += 2;
        return s;
    }

    public int nextInt() {
        int i = getInt(pointer, data);
        pointer += 4;
        return i;
    }

    public long nextLong() {
        long l = getLong(pointer, data);
        pointer += 8;
        return l;
    }

    public char nextChar() {
        char c = getChar(pointer, data);
        pointer += 1;
        return c;
    }

    public byte[] nextBytes(int length) {
        if (length <= 0) {
            return new byte[0];
        }

        if (length + pointer > data.length) {
            throw new IllegalArgumentException("Length for next bytes is out of bounds");
        }

        byte[] bytes = getBytes(pointer, length, data);
        pointer += length;
        return bytes;
    }

    public String nextString() {
        int stringEnd = pointer;

        for (int i = pointer; i < data.length; i++) {
            if (data[i] == 0) {
                stringEnd = i;
                break;
            }
        }

        byte[] bytes = nextBytes(stringEnd - pointer);

        // Skip past null byte
        pointer++;

        return new String(bytes);
    }

    /*
    Format:
        Int16
        Number of columns.

        Next, one of the following submessages appears for each column (except generated columns):

        Byte1('n')
        Identifies the data as NULL value.

        Or

        Byte1('u')
        Identifies unchanged TOASTed value (the actual value is not sent).

        Or

        Byte1('t')
        Identifies the data as text formatted value.

        Or

        Byte1('b')
        Identifies the data as binary formatted value.

        Int32
        Length of the column value.

        Byten
        The value of the column, either in binary or in text format. (As specified in the preceding format byte). n is the above length.
    */

    public List<TupleDataColumn> nextTupleData() {
        short numColumns = nextShort();

        List<TupleDataColumn> columns = new ArrayList<>(numColumns);

        for (short curCol = 0; curCol < numColumns; curCol++) {
            TupleDataColumn.Type type = TupleDataColumn.Type.of(nextChar());

            if (type == TupleDataColumn.Type.NULL) {
                columns.add(new TupleDataColumn(type, null));
                continue;
            } else if (type == TupleDataColumn.Type.TOASTED) {
                continue;
            }

            int columnLength = nextInt();

            byte[] data = null;

            if (columnLength > 0) {
                data = nextBytes(columnLength);
            }

            columns.add(new TupleDataColumn(type, data));
        }

        return columns;
    }

}
