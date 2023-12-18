package com.github.alexgaard.mirror.postgres.test_utils;

import com.github.alexgaard.mirror.postgres.collector.message.RawMessage;

import static com.github.alexgaard.mirror.postgres.utils.ParseUtils.toByteArray;

public class RawEventCreator {

    private static int xid = 1;

    public static RawMessage create(String hexData) {
        return new RawMessage(
                "LSN",
                xid++,
                toByteArray(hexData)
        );
    }

}
