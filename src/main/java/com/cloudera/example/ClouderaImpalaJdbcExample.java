package com.cloudera.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClouderaImpalaJdbcExample {
	
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;

	private static ExecutorService service;


	private static void loadConfiguration() throws IOException {
			InputStream input = null;
			try {
					String filename = ClouderaImpalaJdbcExample.class.getSimpleName() + ".conf";
					input = ClouderaImpalaJdbcExample.class.getClassLoader().getResourceAsStream(filename);
					Properties prop = new Properties();
					prop.load(input);

					jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
			} finally {
					try {
							if (input != null)
									input.close();
					} catch (IOException e) {
							// nothing to do
					}
			}
	}


	private static void init(String sqlStatement ,int concurrency, String host) throws ClassNotFoundException, SQLException {
		service = Executors.newFixedThreadPool(concurrency);
		connectionUrl = "jdbc:impala://" + host;
		Class.forName(jdbcDriverName);

		for(int i=0; i<concurrency; i++){
			Connection conn = DriverManager.getConnection(connectionUrl);
			QueryTask task = new QueryTask("task_"+i ,conn, sqlStatement);
			service.submit(task);
		}

	}


	public static void main(String[] args) throws IOException {

                if (args.length != 3) {
                        System.out.println("Syntax: ClouderaImpalaJdbcExample \"<SQL_query>\" <concurrency> <hostname:port>");
                        System.exit(1);
                }
                String sqlStatement = args[0];
				int concurrency = Integer.valueOf(args[1]);
                String host = args[2];
                loadConfiguration();


		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala JDBC Example");
		System.out.println("Using Connection URL: " + connectionUrl);
		System.out.println("Running Query: " + sqlStatement);
		System.out.println("Concurrency: " + concurrency);

		try {

			init(sqlStatement, concurrency, host);

		} catch(ClassNotFoundException e) {
			System.out.println("Failed to init");
			e.printStackTrace();
		} catch(SQLException e) {
			e.printStackTrace();
		}

		while (true) {
			try {
				Thread.sleep(10000);
			} catch (Exception e) {
				System.out.println("Main thread interupted. Program will exit now");
			}

		}
	}

	private static class QueryTask implements Runnable {
		private Connection conn;
		private String sqlStatement;
		private String name;

		public QueryTask(String name, Connection conn, String sqlStatement) {
			this.name = name;
			this.conn = conn;
			this.sqlStatement = sqlStatement;
		}


		@Override
		public void run() {
			try {
				PreparedStatement stmt = conn.prepareStatement(sqlStatement);
				System.out.println("Thread:" + Thread.currentThread().getName() + " Task:" + this.name + "Connection:" + conn.toString());


				while (true){

					long start = System.currentTimeMillis();
					try {
						ResultSet rs = stmt.executeQuery();
					} catch (SQLException e){
						System.out.println("Thread:" + Thread.currentThread().getName() + " Task:" + this.name + " Failure " + e.getMessage() );
						continue;
					}
					long finish = System.currentTimeMillis();
					System.out.println("Thread:" + Thread.currentThread().getName() + " Task:" + this.name + " Success Duration=" + (finish-start));

				}

			} catch (SQLException e){
				System.out.println("Thread:" + Thread.currentThread().getName() + " Task:" + this.name+ " Failure to prepare statement");
			}
		}
	}
}
