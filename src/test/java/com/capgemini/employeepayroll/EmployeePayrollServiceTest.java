package com.capgemini.employeepayroll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.capgemini.fileio.employeepayrollservice.EmployeePayrollData;
import com.capgemini.fileio.employeepayrollservice.EmployeePayrollException;
import com.capgemini.fileio.employeepayrollservice.EmployeePayrollService;
import com.capgemini.fileio.employeepayrollservice.EmployeePayrollService.IOService;
import com.capgemini.fileio.employeepayrollservice.EmployeePayrollService.NormalisationType;
import com.capgemini.fileio.employeepayrollservice.dbservice.EmployeePayrollDBService.StatementType;

public class EmployeePayrollServiceTest {
	
	@Test
	public void given3Employees_WhenWrittenToFile_ShouldMatchEmployeeEntries() {
		EmployeePayrollData[] arrayOfEmp = {
				new EmployeePayrollData(1,"Jeff Bezos",100000.0),
				new EmployeePayrollData(2, "Bill Gates",200000.0),
				new EmployeePayrollData(3, "Mark Zuckerberg",300000.0)
		};
		EmployeePayrollService employeePayrollService;
		employeePayrollService = new EmployeePayrollService(Arrays.asList(arrayOfEmp));
		employeePayrollService.writeEmployeeData(IOService.FILE_IO);
		long entries = employeePayrollService.countEntries(IOService.FILE_IO);
		employeePayrollService.printData(IOService.FILE_IO);
		List<EmployeePayrollData> employeeList = employeePayrollService.readData(IOService.FILE_IO,NormalisationType.DENORMALISED);
		System.out.println(employeeList);
		assertEquals(3, entries);
	}
	
	@Test
	public void givenEmployeePayrollInDB_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		assertEquals(4, employeePayrollData.size());
	}
	
	@Test
	public void givenNewSalaryForEmployee_WhenUpdated_ShouldSyncWithDatabase() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		employeePayrollService.updateEmployeeSalary("Terisa",3000000.00,StatementType.STATEMENT,NormalisationType.DENORMALISED);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa",NormalisationType.DENORMALISED);
		assertTrue(result);
	}
	
	@Test
	public void givenNewSalaryForEmployee_WhenUpdatedUsingPreparedStatement_ShouldSyncWithDatabase() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		employeePayrollService.updateEmployeeSalary("Terisa",3000000.00,StatementType.PREPARED_STATEMENT,NormalisationType.DENORMALISED);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa",NormalisationType.DENORMALISED);
		assertTrue(result);
	}
	
	@Test
	public void givenDateRangeForEmployee_WhenRetrievedUsingStatement_ShouldReturnProperData() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		List<EmployeePayrollData> employeeDataInGivenDateRange = employeePayrollService.getEmployeesInDateRange("2018-01-01","2019-11-15",NormalisationType.DENORMALISED);
		assertEquals(2, employeeDataInGivenDateRange.size());
	}
	
	//UC6
	@Test
	public void givenPayrollData_WhenAverageSalaryRetrievedByGender_ShouldReturnProperValue() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		Map<String,Double> averageSalaryByGender  = employeePayrollService.readAverageSalaryByGender(IOService.DB_IO,NormalisationType.DENORMALISED);
		System.out.println(averageSalaryByGender);
		assertTrue(averageSalaryByGender.get("M").equals(18000000.00)&&
				averageSalaryByGender.get("F").equals(3000000.00));
	}
	
	@Test
	public void givenNewEmployee_WhenAdded_ShouldSyncWithDB() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		employeePayrollService.addEmployeeToPayroll("Mark",50000000.00,LocalDate.now(),"M");
		boolean result =  employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark",NormalisationType.DENORMALISED);
		assertTrue(result);
	}
	
	//Multithreading UC1 to UC4
	@Test
	public void given6Employees_WhenAddedToDB_ShouldMatchEmployeeEntries() {
		EmployeePayrollData[] arrayOfEmps = {
				new EmployeePayrollData(0,"Jeff Bezos","M",100000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Bill Gates","M",200000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Mark Zuckerberg","M",300000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Sunder","M",600000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Mukesh","M",100000.0,LocalDate.now()),
				new EmployeePayrollData(0,"Anil","M",200000.0,LocalDate.now())
		};
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO, NormalisationType.DENORMALISED);
		Instant start = Instant.now();
		employeePayrollService.addEmployeesToPayroll(Arrays.asList(arrayOfEmps));
		Instant end = Instant.now();
		Instant threadStart = Instant.now();		
		//employeePayrollService.addEmployeesToPayrollWithThreads(Arrays.asList(arrayOfEmps));
		Instant threadEnd = Instant.now();	
		System.out.println("Duration with thread: "+Duration.between(threadStart, threadEnd));
		System.out.println("Duration without thread: "+Duration.between(start, end));
		employeePayrollService.readData(IOService.DB_IO, NormalisationType.DENORMALISED);
		assertEquals(16, employeePayrollService.countEntries(IOService.DB_IO));
	}
	
	//MultiThreading UC6
	@Test
	public void given3Employees_WhenDetailsAreUpdated_ShouldSyncWithDB() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.DENORMALISED);
		Map<String,Double> updationDetailsMap = new HashMap<String, Double>();
		updationDetailsMap.put("Jeff Bezos", 2000000.00);
		updationDetailsMap.put("Bill Gates", 3000000.00);
		updationDetailsMap.put("Mark Zuckerberg", 4000000.00);
		employeePayrollService.updateEmployeesSalaryWithThreads(updationDetailsMap,StatementType.STATEMENT,NormalisationType.DENORMALISED);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa",NormalisationType.DENORMALISED);
		assertTrue(result);
	}
	
	//TESTS FOR NORMALISED TABLES
	@Test
	public void givenEmployeePayrollInNormalisedDB_WhenRetrieved_ShouldMatchEmployeeCount() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		System.out.println(employeePayrollData);
		for(EmployeePayrollData emp : employeePayrollData ) {
			emp.printDepartments();
		}
		assertEquals(3, employeePayrollData.size());
	}
	
	@Test
	public void givenNewSalaryForEmployeeInNormalisedDB_WhenUpdated_ShouldSyncWithDatabase() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		employeePayrollService.updateEmployeeSalary("Terisa",3000000.00,StatementType.STATEMENT,NormalisationType.NORMALISED);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa",NormalisationType.NORMALISED);
		assertTrue(result);
	}
	
	@Test
	public void givenNewSalaryForEmployeeInNormalisedDB_WhenUpdatedUsingPreparedStatement_ShouldSyncWithDatabase() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		employeePayrollService.updateEmployeeSalary("Terisa",3000000.00,StatementType.PREPARED_STATEMENT,NormalisationType.NORMALISED);
		boolean result = employeePayrollService.checkEmployeePayrollInSyncWithDB("Terisa",NormalisationType.NORMALISED);
		assertTrue(result);
	}
	
	@Test
	public void givenDateRangeForEmployeeInNormalisedDB_WhenRetrievedUsingStatement_ShouldReturnProperData() throws EmployeePayrollException {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		List<EmployeePayrollData> employeePayrollData = employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		List<EmployeePayrollData> employeeDataInGivenDateRange = employeePayrollService.getEmployeesInDateRange("2018-01-01","2019-11-15",NormalisationType.NORMALISED);
		assertEquals(2, employeeDataInGivenDateRange.size());
	}
	
	@Test
	public void givenPayrollDataForNormalisedDB_WhenAverageSalaryRetrievedByGender_ShouldReturnProperValue() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		Map<String,Double> averageSalaryByGender  = employeePayrollService.readAverageSalaryByGender(IOService.DB_IO,NormalisationType.NORMALISED);
		System.out.println(averageSalaryByGender);
		assertTrue(averageSalaryByGender.get("M").equals(55000.00)&&
				averageSalaryByGender.get("F").equals(3000000.00));
	}
	
	@Test
	public void givenNewEmployee_WhenAddedToNormalisedDB_ShouldSyncWithDB() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		employeePayrollService.addEmployeeToPayroll(104, "Mark", 5000000.00, LocalDate.now(), "M", "Company D", 4, new int[] {201});
		boolean result =  employeePayrollService.checkEmployeePayrollInSyncWithDB("Mark",NormalisationType.NORMALISED);
		assertTrue(result);
	}
	
	@Test
	public void givenEmployee_WhenRemoved_ShouldSetIsActiveToFalse() {
		EmployeePayrollService employeePayrollService = new EmployeePayrollService();
		employeePayrollService.readData(IOService.DB_IO,NormalisationType.NORMALISED);
		boolean result =  employeePayrollService.removeEmployee(103);
		assertTrue(result);
	}
}
