package com.capgemini.employeepayroll;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

/**
 * @author Rachit 
 * JDBC
 */
public class EmployeePayroll {
	public static void main(String[] args) {
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "root";
		Connection connection;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Driver loaded");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Cannot find driver in classpath");
		}
		listDrivers();
		try {
			System.out.println("Connecting to database: " + jdbcURL);
			connection = DriverManager.getConnection(jdbcURL, userName, password);
			System.out.println("Connection successful: " + connection);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * List the drivers being loaded
	 */
	private static void listDrivers() {
		Enumeration<Driver> driverList = DriverManager.getDrivers();
		while (driverList.hasMoreElements()) {
			Driver driverClass = (Driver) driverList.nextElement();
			System.out.println("   " + driverClass.getClass().getName());
		}
	}
}
