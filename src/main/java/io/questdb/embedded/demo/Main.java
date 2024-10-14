package io.questdb.embedded.demo;

import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;

public class Main {
    public static void main(String[] args) throws SqlException, NumericException {
        String data_dir = "data_dir/db";
        TableCreator.createTable(data_dir);
        TableWriter.insertRows(data_dir, 2);
        QueryRunner.run(data_dir);
    }
}
