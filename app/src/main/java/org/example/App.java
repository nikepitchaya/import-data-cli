package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

public class App {

    public static void main(String[] args) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.print("1.WCF\n2.REG\nSelect Schema Connection <Default 1.WCF>: ");
        String schema = scanner.nextLine();
        System.out.print("Path of the CSV file: ");
        String path = scanner.nextLine();
        String tableName = path.split("/")[path.split("/").length - 1].split("\\.")[0];
        Connection connection = checkDatabaseConnection(schema);
        try {
            CsvMultithreadingService service = new CsvMultithreadingService(connection);
            List<String> messages = service.readCsvAndInsertData(path, tableName);
            for (String message : messages)
                System.out.println(message);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanner.close();
            connection.close();
        }

    }

    public static Connection checkDatabaseConnection(String schema) throws SQLException {
        try {
            String jdbcUrl = "";
            String jdbcUser = "";
            String jdbcPassword = "";
            switch (schema) {
                case "2":
                    jdbcUrl = "jdbc:oracle:thin:@//143.198.95.207:1521/PDB2";
                    jdbcUser = "pdb_adm";
                    jdbcPassword = "Password1";
                    break;
                default:
                    jdbcUrl = "jdbc:oracle:thin:@//143.198.95.207:1521/PDBMED";
                    jdbcUser = "wcf_med";
                    jdbcPassword = "wC4_m3d1c0l";
                    break;
            }
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            return connection;
        } catch (Exception e) {
            System.out.println("Connect Database Failed: " + e.getMessage());
            throw new SQLException("Connection failed");
        }

    }
}
