package io.questdb.embedded.demo;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.NumericException;
import io.questdb.std.Os;

public class TableWriter {
    public static void insertRows(String data_dir, int numberOfRows) throws SqlException, NumericException {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(data_dir);

        try (CairoEngine engine = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE, null);

            // write data into WAL
            final TableToken tableToken = engine.getTableTokenIfExists("testTable");
            try (TableWriterAPI writer = engine.getTableWriterAPI(tableToken, "testTable")) {
                for (int i = 0; i < numberOfRows; i++) {
                    io.questdb.cairo.TableWriter.Row row = writer.newRow(Os.currentTimeMicros());
                    row.putInt(0, 123);
                    row.putByte(1, (byte) 1111);
                    row.putShort(2, (short) 222);
                    row.putLong(3, 333);
                    row.putFloat(4, 4.44f);
                    row.putDouble(5, 5.55);
                    row.putDate(6, System.currentTimeMillis());
                    row.putSym(7, "xyz");
                    row.putStr(8, "abc");
                    row.putBool(9, true);
                    row.putGeoHash(10, GeoHashes.fromString("u33dr01d", 0, 8));
                    row.append();
                }
                writer.commit();
            }

            // apply WAL to the table
            try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1)) {
                while (walApplyJob.run(0)) ;
            }
        }
    }
}
