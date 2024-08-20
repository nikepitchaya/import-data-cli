package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvMultithreadingService {

	private final Connection connection;

	public CsvMultithreadingService(Connection connection) {
		this.connection = connection;
	}

	public List<String> readCsvAndInsertData(String filePath, String tableName)
			throws CsvException, InterruptedException {
		System.out.println("readCsvAndInsertData: " + filePath + tableName);
		List<String> message = new ArrayList<>();
		try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
			List<String[]> csvData = reader.readAll();
			String[] headers = csvData.get(0);
			List<String[]> records = csvData.subList(1, csvData.size());
			int indexForIsDeleted = Arrays.asList(headers).indexOf("IS_DELETED");
			String insertSql = createInsertSql(tableName, headers);
			int maxThread = 10;
			if (records.size() < maxThread) {
				maxThread = records.size();
			}
			int maxL = (int) Math.ceil((double) records.size() / maxThread);
			List<List<String[]>> chunks = new ArrayList<>();
			for (int i = 0; i < maxThread; i++) {
				int start = i * maxL;
				int end = Math.min(start + maxL, records.size());
				if (start < records.size()) {
					chunks.add(records.subList(start, end));
				}
			}
			ExecutorService executor = Executors.newFixedThreadPool(maxThread);
			for (List<String[]> chunk : chunks) {
				executor.submit(() -> {
					for (String[] item : chunk) {
						List<Object> values = processLine(headers, item, indexForIsDeleted);
						updateJdbc(connection, insertSql, values, message);
					}
				});
			}
			executor.shutdown();
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		return message;
	}

	private void updateJdbc(Connection connection, String insertSql, List<Object> values, List<String> message) {
		try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
			for (int i = 0; i < values.size(); i++) {
				ps.setObject(i + 1, values.get(i));
			}
			ps.executeUpdate();
			message.add("SUCCESS " + values);
		} catch (SQLException e) {
			message.add("ERROR " + values + " (" + e.getMessage() + ")");
			e.printStackTrace();
		}
	}

	private List<Object> processLine(String[] headers, String[] nextLine, int indexForIsDeleted) {
		ArrayList<Object> values = new ArrayList<>();
		for (int i = 0; i < headers.length; i++) {
			if (i == indexForIsDeleted) {
				values.add(convertToIsActive(nextLine, i));
				continue;
			}
			if (headers[i].contains("_DATE") || headers[i].contains("_TIME") || headers[i].equals("BIRTHDATE")) {
				values.add(DateFormatUtil.parseDateStringToDateSql(nextLine[i]));
			} else {
				values.add(nextLine[i]);
			}
		}
		if (indexForIsDeleted == -1) {
			values.add("Y");
		}
		values.add(UUID.randomUUID().toString());
		return values;
	}

	private static String convertToIsActive(String[] nextLine, int i) {
		return nextLine[i].equalsIgnoreCase("N") ? "Y" : "N";
	}

	private String createInsertSql(String tableName, String[] headers) {
		String searchText = "IS_DELETED";
		String newField = "IS_ACTIVE";
		String uuidField = "UUID";
		int indexForIsDeleted = Arrays.asList(headers).indexOf(searchText);
		if (indexForIsDeleted > 0) {
			headers[indexForIsDeleted] = newField;
			headers = Arrays.copyOf(headers, headers.length + 1);
			headers[headers.length - 1] = uuidField;
		} else {
			headers = Arrays.copyOf(headers, headers.length + 2);
			headers[headers.length - 2] = newField;
			headers[headers.length - 1] = uuidField;
		}
		String columns = String.join(", ", headers);
		String placeholders = IntStream.range(0, headers.length)
				.mapToObj(i -> "?")
				.collect(Collectors.joining(", "));
		return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
	}
}
