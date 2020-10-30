package com.capgemini.fileio.employeepayrollservice.dbservice;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.capgemini.fileio.employeepayrollservice.EmployeePayrollData;
import com.capgemini.fileio.employeepayrollservice.dbservice.EmployeePayrollDBService.StatementType;

public class EmployeePayrollDBServiceNormalised {

	private static EmployeePayrollDBServiceNormalised employeePayrollDBServiceNormalised;

	private PreparedStatement employeePayrollDataStatementNormalised;

	private EmployeePayrollDBServiceNormalised() {
	}

	/**
	 * @return singleton instance
	 */
	public static EmployeePayrollDBServiceNormalised getInstance() {
		if (employeePayrollDBServiceNormalised == null)
			employeePayrollDBServiceNormalised = new EmployeePayrollDBServiceNormalised();
		return employeePayrollDBServiceNormalised;
	}

	/**
	 * @return employee list read from database
	 */
	public List<EmployeePayrollData> readData() {
		String sql = "SELECT e.id,e.company_id,e.employee_name,e.gender,e.start,c.company_name,d.dept_name,p.basic_pay "
				+ "FROM employee e JOIN company c" + " ON e.company_id = c.company_id " + "JOIN employee_department d2 "
				+ "ON e.id = d2.emp_id " + "JOIN department d " + "ON d2.dept_id = d.dept_id " + "JOIN payroll p "
				+ "ON e.id = p.emp_id "
				+ "WHERE e.is_active = '1';";
		return this.getEmployeePayrollDataUsingSQLQuery(sql);
	}

	/**
	 * @param sql
	 * @return employee payroll list
	 */
	private List<EmployeePayrollData> getEmployeePayrollDataUsingSQLQuery(String sql) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		try (Connection connection = this.getConnection()) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}

	/**
	 * @param resultSet
	 * @return employee payroll list
	 */
	private List<EmployeePayrollData> getEmployeePayrollData(ResultSet resultSet) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<>();
		List<String> department = new ArrayList<String>();
		try {
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				int companyId = resultSet.getInt("company_id");
				String name = resultSet.getString("employee_name");
				String gender = resultSet.getString("gender");
				LocalDate startDate = resultSet.getDate("start").toLocalDate();
				String companyName = resultSet.getString("company_name");
				String dept = resultSet.getString("dept_name");
				double salary = resultSet.getDouble("basic_pay");
				department.add(dept);
				String[] departmentArray = new String[department.size()];
				EmployeePayrollData employee = new EmployeePayrollData(id, name, salary, startDate, gender, companyName,
						companyId, department.toArray(departmentArray));
				if (employeePayrollList.stream().anyMatch(emp -> emp.equals(employee)))
					employee.setDepartment(department.toArray(departmentArray));
				else {
					employeePayrollList.add(employee);
					department = new ArrayList<String>();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}
	
	/**
	 * @param name
	 * @return employee payroll list
	 */
	public List<EmployeePayrollData> getEmployeePayrollData(String name) {
		List<EmployeePayrollData> employeePayrollList = null;
		if (this.employeePayrollDataStatementNormalised == null)
			this.preparedStatementForEmployeeData();
		try {
			employeePayrollDataStatementNormalised.setString(1, name);
			ResultSet resultSet = employeePayrollDataStatementNormalised.executeQuery();
			employeePayrollList = this.getEmployeePayrollData(resultSet);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return employeePayrollList;
	}
	
	/**
	 * Prepared statement to get employee data of a particular employee
	 */
	private void preparedStatementForEmployeeData() {
		try {
			Connection connection = this.getConnection();
			String sql = "SELECT e.id,e.company_id,e.employee_name,e.gender,e.start,c.company_name,d.dept_name,p.basic_pay "
					+ "FROM employee e JOIN company c" + " ON e.company_id = c.company_id " + "JOIN employee_department d2 "
					+ "ON e.id = d2.emp_id " + "JOIN department d " + "ON d2.dept_id = d.dept_id " + "JOIN payroll p "
					+ "ON e.id = p.emp_id WHERE e.employee_name = ?";
			employeePayrollDataStatementNormalised = connection.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param name
	 * @param salary
	 * @param type
	 * @return number of updated entries
	 */
	public int updateEmployeeData(String name, double salary, StatementType type) {
		switch (type) {
		case STATEMENT:
			return this.updateDataUsingStatement(name, salary);
		case PREPARED_STATEMENT:
			return this.updateDataUsingPreparedStatement(name, salary);
		default:
			return 0;
		}
	}

	/**
	 * @param name
	 * @param salary
	 * @return no of rows affected
	 */
	private int updateDataUsingPreparedStatement(String name, double salary) {
		String sql = "UPDATE payroll SET basic_pay = ? WHERE emp_id = "
				+ "(SELECT id from employee WHERE employee_name = ?);";
		try (Connection connection = this.getConnection();) {
			PreparedStatement preparedStatementUpdate = connection.prepareStatement(sql);
			preparedStatementUpdate.setDouble(1, salary);
			preparedStatementUpdate.setString(2, name);
			return preparedStatementUpdate.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * @param name
	 * @param salary
	 * @return no of rows affected
	 */
	private int updateDataUsingStatement(String name, double salary) {
		String sql = String.format("UPDATE payroll SET basic_pay = %.2f WHERE emp_id = "
				+ "(SELECT id from employee WHERE employee_name = '%s');", salary, name);
		try (Connection connection = this.getConnection();) {
			Statement statement = connection.createStatement();
			return statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public List<EmployeePayrollData> getEmployeePayrollDataInGivenDateRangeDB(String date1, String date2) {
		String sql = String.format("SELECT e.id,e.company_id,e.employee_name,e.gender,e.start,c.company_name,d.dept_name,p.basic_pay "
				+ "FROM employee e JOIN company c" + " ON e.company_id = c.company_id " + "JOIN employee_department d2 "
				+ "ON e.id = d2.emp_id " + "JOIN department d " + "ON d2.dept_id = d.dept_id " + "JOIN payroll p "
				+ "ON e.id = p.emp_id "
				+ "WHERE WHERE e.is_active = '1' AND e.start BETWEEN '%s' AND '%s';",date1,date2);
		return this.getEmployeePayrollDataUsingSQLQuery(sql);
	}
	
	public Map<String, Double> getAverageSalaryByGender() {
		String sql = "SELECT e.gender,AVG(p.basic_pay) "
				+ "FROM employee e join payroll p "
				+ "ON e.id = p.emp_id where e.is_active = '1' "
				+ "GROUP BY e.gender;";
		Map<String,Double> genderToAvgSalaryMap = new HashMap<String, Double>();
		try(Connection connection = this.getConnection()){
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while(resultSet.next()) {
				String gender = resultSet.getString("gender");
				double salary = resultSet.getDouble("AVG(p.basic_pay)");
				genderToAvgSalaryMap.put(gender, salary);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return genderToAvgSalaryMap;
	}
	
	public EmployeePayrollData addEmployeeToPayroll(int id, String name, double salary, LocalDate startDate,
			String gender, String companyName, int companyId, int[] department) {
		Connection connection = null;
		EmployeePayrollData employeePayrollData = null;
		try {
			connection = this.getConnection();
			connection.setAutoCommit(false);
		}catch(SQLException e) {
			e.printStackTrace();
		}
		//Statement 1
		try(Statement statement = connection.createStatement()){
			String sql = String.format("INSERT INTO company VALUES (%s,'%s')", companyId,companyName);
			int rowAffected = statement.executeUpdate(sql);
			
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				return employeePayrollData;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		
		//Statement 2
		try(Statement statement = connection.createStatement()){
			String sql = String.format("INSERT INTO employee VALUES( %s, %s,'%s','%s','%s','%s')", 
					id,companyId,name,gender,Date.valueOf(startDate),"1");
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				return employeePayrollData;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		
		//Statement 3
		try(Statement statement = connection.createStatement()){
			double deductions = salary*0.2;
			double taxablePay = salary-deductions;
			double tax = taxablePay*0.1;
			double netPay = salary - tax;
			String sql =  String.format("INSERT INTO payroll (emp_id,basic_pay,deductions,taxable_pay,tax,net_pay) VALUES"
					+ "( %s, %s, %s ,%s, %s, %s)",id,salary,deductions,taxablePay,tax,netPay);
			int rowAffected = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				return employeePayrollData;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		
		//Statement 4
		try(Statement statement = connection.createStatement()){
			int rowsAffected=0;
			for(int dept: department) {
				String sql = String.format("INSERT INTO employee_department VALUES (%s,%s)",id,dept);
				int rowAffected = statement.executeUpdate(sql);
				if(rowAffected==1)
					rowsAffected++;
			}
			if(rowsAffected>0)
				employeePayrollData = new EmployeePayrollData(id, name, salary, startDate, gender, 
						companyName, companyId, new String[] {"Sales"});
		} catch (SQLException e) {
			e.printStackTrace();
			try {
				connection.rollback();
				return employeePayrollData;
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		
		//Final commit
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(connection!=null)
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
		return employeePayrollData;
	}

	private Connection getConnection() throws SQLException {
		String jdbcURL = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
		String userName = "root";
		String password = "root";
		Connection connection;
		System.out.println("Connecting to database: " + jdbcURL);
		connection = DriverManager.getConnection(jdbcURL, userName, password);
		System.out.println("Connection successful: " + connection);
		return connection;
	}
}
