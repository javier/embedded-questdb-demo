package io.questdb.embedded.demo;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.NumericException;
import io.questdb.std.Os;

public class TableCreator {
    public static void createTable(String data_dir) throws SqlException, NumericException {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(data_dir);

        try (CairoEngine engine = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE, null);
            engine.ddl("CREATE TABLE IF NOT EXISTS testTable (" +
                    "a int, b byte, c short, d long, e float, g double, h date, " +
                    "i symbol, j string, k boolean, l geohash(8c), ts timestamp" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL", ctx);
        }
    }
}
