package com.capgemini.fileio.employeepayrollservice;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import com.capgemini.fileio.employeepayrollservice.EmployeePayrollException.ExceptionType;
import com.capgemini.fileio.employeepayrollservice.dbservice.EmployeePayrollDBService;
import com.capgemini.fileio.employeepayrollservice.dbservice.EmployeePayrollDBService.StatementType;
import com.capgemini.fileio.employeepayrollservice.dbservice.EmployeePayrollDBServiceNormalised;

import java.util.Map;

public class EmployeePayrollService {
	public enum IOService {
		CONSOLE_IO, FILE_IO, DB_IO, REST_IO
	}
	
	public enum NormalisationType{
		NORMALISED,DENORMALISED
	}

	public List<EmployeePayrollData> employeePayrollList;
	private EmployeePayrollDBService employeePayrollDBService;
	private EmployeePayrollDBServiceNormalised employeePayrollDBServiceNormalised;

	public EmployeePayrollService() {
		employeePayrollDBService = EmployeePayrollDBService.getInstance();
		employeePayrollDBServiceNormalised = EmployeePayrollDBServiceNormalised.getInstance();
	}

	public EmployeePayrollService(List<EmployeePayrollData> employeePayrollList) {
		this();
		this.employeePayrollList = new ArrayList<>(employeePayrollList);
		
	}

	public static void main(String[] args) {
		List<EmployeePayrollData> employeePayrollList = new ArrayList<EmployeePayrollData>();
		EmployeePayrollService employeePayrollService = new EmployeePayrollService(employeePayrollList);
		Scanner consoleInputReader = new Scanner(System.in);
		employeePayrollService.readEmployeeData(consoleInputReader);
		employeePayrollService.writeEmployeeData(IOService.CONSOLE_IO);
	}

	/**
	 * @param consoleInputReader Read employee data
	 */
	public void readEmployeeData(Scanner consoleInputReader) {
		System.out.println("Enter employee ID : ");
		int id = Integer.parseInt(consoleInputReader.nextLine());
		System.out.println("Enter employee name : ");
		String name = consoleInputReader.nextLine();
		System.out.println("Enter employee salary : ");
		double salary = Double.parseDouble(consoleInputReader.nextLine());
		employeePayrollList.add(new EmployeePayrollData(id, name, salary));
	}

	/**
	 * Write payroll data to console
	 */
	public void writeEmployeeData(IOService ioService) {
		if (ioService.equals(IOService.CONSOLE_IO))
			System.out.println("Writing Employee Payroll Data to Console\n" + employeePayrollList);
		else if (ioService.equals(IOService.FILE_IO))
			new EmployeePayrollFileIOService().writeData(employeePayrollList);
	}

	/**
	 * @param ioService Print Data
	 */
	public void printData(IOService ioService) {
		new EmployeePayrollFileIOService().printData();
	}

	/**
	 * @param ioService
	 * @return number of entries
	 */
	public long countEntries(IOService ioService) {
		if (ioService.equals(IOService.FILE_IO))
			return new EmployeePayrollFileIOService().countEntries();
		return employeePayrollList.size();
	}
	
	/**
	 * @param ioService
	 * @return Employee Payroll Data List
	 */
	public List<EmployeePayrollData> readData(IOService ioService,NormalisationType normalisationType) {
		if(ioService.equals(IOService.FILE_IO))
			 return new EmployeePayrollFileIOService().readData();
		else if(ioService.equals(IOService.DB_IO)) {
			if(normalisationType.equals(NormalisationType.DENORMALISED)) {
				employeePayrollList = employeePayrollDBService.readData();
			}
			else if(normalisationType.equals(NormalisationType.NORMALISED)) {
				employeePayrollList = employeePayrollDBServiceNormalised.readData();
			}
			return employeePayrollList;
		}
		else
			return null;
	}

	/**
	 * @param name
	 * @return Employee corresponding to name
	 */
	public EmployeePayrollData getEmployeePayrollData(String name) {
		EmployeePayrollData employeePayrollData = this.employeePayrollList.stream()
				.filter(employee->employee.name.equals(name))
				.findFirst()
				.orElse(null);
		return employeePayrollData;
	}

	/**
	 * @param name
	 * @return true if data is in sync
	 */
	public boolean checkEmployeePayrollInSyncWithDB(String name,NormalisationType normalisationType) {
		List<EmployeePayrollData> checkList = null;
		if(normalisationType.equals(NormalisationType.DENORMALISED)) 
			checkList = employeePayrollDBService.getEmployeePayrollData(name);
		else if(normalisationType.equals(NormalisationType.NORMALISED)) 
			checkList = employeePayrollDBServiceNormalised.getEmployeePayrollData(name);
		return checkList.get(0).equals(getEmployeePayrollData(name));
		
	}

	/**
	 * @param date1
	 * @param date2
	 * @return employee list in given date range
	 */
	public List<EmployeePayrollData> getEmployeesInDateRange(String date1, String date2,NormalisationType normalisationType) {
		List<EmployeePayrollData> employeesInGivenDateRangeList = null;
		if(normalisationType.equals(NormalisationType.DENORMALISED))
			employeesInGivenDateRangeList = employeePayrollDBService.getEmployeesInGivenDateRangeDB(date1,date2);
		else if(normalisationType.equals(NormalisationType.NORMALISED))
			employeesInGivenDateRangeList = employeePayrollDBServiceNormalised.getEmployeePayrollDataInGivenDateRangeDB(date1,date2);
		return employeesInGivenDateRangeList;
	}

	/**
	 * @param ioService
	 * @return Employee name and avg salary map
	 */
	public Map<String, Double> readAverageSalaryByGender(IOService ioService,NormalisationType normalisationType) {
		if(ioService.equals(IOService.DB_IO)) {
			if(normalisationType.equals(NormalisationType.DENORMALISED))
				return employeePayrollDBService.getAverageSalaryByGender();
			else if(normalisationType.equals(NormalisationType.NORMALISED))
				return employeePayrollDBServiceNormalised.getAverageSalaryByGender();
		}
		return null;
	}

	/**
	 * @param name
	 * @param salary
	 * @param startDate
	 * @param gender
	 * For denormalised tables
	 */
	public void addEmployeeToPayroll(String name, double salary, LocalDate startDate, String gender) {
		employeePayrollList.add(employeePayrollDBService.addEmployeeToPayroll(name,salary,startDate,gender));
	}
	
	/**
	 * @param id
	 * @param name
	 * @param salary
	 * @param startDate
	 * @param gender
	 * @param companyName
	 * @param companyId
	 * @param departments
	 * For normalised tables
	 */
	public void addEmployeeToPayroll(int id, String name, double salary, LocalDate startDate,
			String gender,String companyName,int companyId,int departments[]) {
		employeePayrollList.add(employeePayrollDBServiceNormalised.addEmployeeToPayroll(id,name,salary,startDate,
				gender,companyName,companyId,departments));
	}
	
	public void addEmployeeToPayroll(EmployeePayrollData employeePayrollData,IOService ioService) {
		if(ioService.equals(IOService.DB_IO))
			this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary, employeePayrollData.startDate, 
					employeePayrollData.gender);
		else employeePayrollList.add(employeePayrollData);
	}

	/**
	 * @param employeePayrollDataList
	 * Multithreading UC1
	 */
	public void addEmployeesToPayroll(List<EmployeePayrollData> employeePayrollDataList) {
		employeePayrollDataList.forEach(employeePayrollData->{
			System.out.println("Employee being added: "+employeePayrollData.name);
			this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary, 
					employeePayrollData.startDate, employeePayrollData.gender);
			System.out.println("Employee added: "+employeePayrollData.name);
		});
		System.out.println(employeePayrollDataList);
	}
	
	/**
	 * @param asList
	 * Multithreading UC2
	 */
	public void addEmployeesToPayrollWithThreads(List<EmployeePayrollData> empList) {
		Map<Integer,Boolean> employeeAdditionStatus = new HashMap<Integer, Boolean>();
		empList.forEach(employeePayrollData -> {
			Runnable task = () -> {
				employeeAdditionStatus.put(employeePayrollData.hashCode(), false);
				System.out.println("Employee being added:(threads) "+Thread.currentThread().getName());
				this.addEmployeeToPayroll(employeePayrollData.name, employeePayrollData.salary, 
						employeePayrollData.startDate, employeePayrollData.gender);
				employeeAdditionStatus.put(employeePayrollData.hashCode(), true);
				System.out.println("Employee added: (threads)"+Thread.currentThread().getName());
			};
			Thread thread = new Thread(task,employeePayrollData.name);
			thread.start();
		});
		while(employeeAdditionStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println(employeePayrollList);
	}
	

	/**
	 * @param name
	 * @param salary
	 * @throws EmployeePayrollException 
	 */
	public void updateEmployeeSalary(String name, double salary,StatementType type,NormalisationType normalisationType)  {
		int result = 0;
		if(normalisationType.equals(NormalisationType.DENORMALISED)) {
			result = employeePayrollDBService.updateEmployeeData(name,salary,type);
		}
		else if(normalisationType.equals(NormalisationType.NORMALISED)) {
			result = employeePayrollDBServiceNormalised.updateEmployeeData(name,salary,type);
		}
			EmployeePayrollData employeePayrollData = null;
			if(result == 0)
				return;
			else 
				 employeePayrollData = this.getEmployeePayrollData(name);
			if(employeePayrollData!=null) {
				employeePayrollData.salary = salary;
			}
	}

	/**
	 * @param updationDetailsMap
	 * @param statementType
	 * @param normalisationType
	 * Update employee salary using threads
	 */
	public void updateEmployeesSalaryWithThreads(Map<String,Double> updationDetailsMap, StatementType statementType,
			NormalisationType normalisationType) {
		Map<String,Boolean> employeeUpdationStatus = new HashMap<String, Boolean>();
		updationDetailsMap.entrySet().forEach(entry->{
			Runnable task = () -> {
				employeeUpdationStatus.put(entry.getKey(), false);
				System.out.println("Employee being updated:(thread) "+Thread.currentThread().getName());
					this.updateEmployeeSalary(entry.getKey(), entry.getValue(), statementType, normalisationType);
					employeeUpdationStatus.put(entry.getKey(), true);
					System.out.println("Employee updated: (thread) "+Thread.currentThread().getName());
			};
			Thread thread = new Thread(task,entry.getKey());
			thread.start();
		});
		while(employeeUpdationStatus.containsValue(false)) {
			try {
				Thread.sleep(10);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean removeEmployee(int id) {
		int result = employeePayrollDBServiceNormalised.removeEmployee(id);
		if(result==1)
			return true;
		else 
			return false;
	}

	/**
	 * UC3 REST 
	 * @param name
	 * @param salary
	 * @param ioService
	 */
	public void updateEmployeeSalary(String name, double salary, IOService ioService) {
		if(ioService.equals(IOService.REST_IO)) {
			EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
			if(employeePayrollData!=null)
				employeePayrollData.salary = salary;
		}
	}

	/**
	 * UC5 REST Delete an employee's data
	 * @param name
	 * @param restIo
	 */
	public void deleteEmployeePayrollJSONServer(String name, IOService ioService) {
		if(ioService.equals(IOService.REST_IO)) {
			EmployeePayrollData employeePayrollData = this.getEmployeePayrollData(name);
			employeePayrollList.remove(employeePayrollData);
		}
	}
}
