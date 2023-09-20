package io.questdb.embedded.demo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;

public class QueryRunner {
    public static void run(String data_dir) throws SqlException {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(data_dir);
        try (CairoEngine engine = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE, null);
            try (RecordCursorFactory factory = engine.select("SELECT * FROM testTable", ctx)) {
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        System.out.printf("a: %s b: %s c: %s d: %s e: %s f: %s g: %s h: %s i: %s j: %s k: %s \n",
                        record.getInt(0),
                        record.getByte(1),
                        record.getShort(2),
                        record.getLong(3),
                        record.getFloat(4),
                        record.getDouble(5),
                        record.getDate(6),
                        record.getSym(7),
                        record.getStr(8),
                        record.getBool(9),
                        record.getGeoLong(10)
                        );

                    }
                }
            }
        }
    }
}

