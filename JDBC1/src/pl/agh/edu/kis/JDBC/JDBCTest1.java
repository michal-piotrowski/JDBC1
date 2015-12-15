package pl.agh.edu.kis.JDBC;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

public class JDBCTest1 {
    private static final String DB_URL = "jdbc:h2:tcp://localhost/~/test";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWD = "sa";
  //-------------------------------------------------------------------
  //-------------------------------------------------------------------
    
    public static void main(String[] args) throws Exception {
    	/*
        // wczytanie sterownika bazy danych (z pliku h2-[wersja].jar)
    	
        
             

        // kod aplikacji
        //insertEmployee(conn, "Jan", "Kowalski");
         //wersja 2
//        Date dateOfBirth = createDate(1990, 4, 21);
//        insertEmployeeV2(conn,"Anna", "Nowak", dateOfBirth);        
        insertPensja(conn, 101, 2100);
        insertPensja(conn, 102, 2300);
        insertEmployee(conn, "Jan", "Kolejny");
        insertPensjaPracownik(conn, "Bogumi³a", "Bia³ow¹s", 1213, 3400);        
        insertPensjaPracownik(conn, "Katarzyna", "Kos", 1214, 3200);
       
        //printEmployeeList(conn);
        
        insertPensjaPracownik(conn, "Katarzyna", "Kos", 1214, 3200);
        insertPensjaPracownik(conn, "Bogumi³a", "Bia³ow¹s", 1213, 3400);
        insertPensjaPracownik(conn, "Andrzej", "Kawa³ek", 1215, 2300);
        insertPensjaPracownik(conn, "Anna", "Zybert", 1216, 4700);
        insertPensjaPracownik(conn, "Han", "Solo", 1217, 6700);
        List<Employee> holdEmployees = findEmployeeBySurname(conn, "Solo");
        
        removeEmployee(conn, 56);*/
    	Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWD);   
        clear(conn);
        
        insertPensjaPracownik(conn, "Katarzyna", "Kos", 1214, 3200);
        insertPensjaPracownik(conn, "Bogumi³a", "Bia³ow¹s", 1213, 3400);
        insertPensjaPracownik(conn, "Andrzej", "Kawa³ek", 1215, 2300);
        insertPensjaPracownik(conn, "Anna", "Zybert", 1216, 4700);
        insertPensjaPracownik(conn, "Han", "Solo", 1217, 6700);
        insertPensjaPracownik(conn, "Bo¿ydar", "Bia³ow¹s", 1220, 3500);
        insertPensjaPracownik(conn, "Krzysztof", "W¹ski", 1219, 3700);
        insertPensjaPracownik(conn, "Aleksander", "Wojewódzki", 1001, 11200);
        insertPensjaPracownik(conn, "Natalia", "Bia³ow¹s", 1012, 9800);
        
        List<Employee> newemp = findEmployeeBySurname(conn, "Bia³ow¹s");
        for(Employee i: newemp)
        	printEmployee(i);
        
        
        // zamkniecie polaczenia z baza
        conn.close();
        System.out.println("Done.");
    }
   
  //-------------------------------------------------------------------
  //-------------------------------------------------------------------  
 // pobieranie liczby wszystkich pracownikow w bazie
    private static int getNoOfEmployees(Connection dbConnection) {
        int noOfEmplyees = -1;

        PreparedStatement stmt = null;
        try {
            stmt = dbConnection
                    .prepareStatement("select count(*) from pracownik");
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                noOfEmplyees = rs.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return noOfEmplyees;
    }
    
    //-------------------------------------------------------------------
    //------------------------------------------------------------------- 
    private static void removeEmployee(Connection dbConnection, int employeeId) {
        PreparedStatement stmt = null;
        try {
            stmt = dbConnection
                    .prepareStatement("delete from pracownik where id=?");
            stmt.setInt(1, employeeId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
 // wyszukuje pracownikow w bazie po nazwisku
    private static java.util.List<Employee> findEmployeeBySurname(
            Connection dbConnection, String surname) {
        java.util.List<Employee> employeeList = new ArrayList<Employee>();

        PreparedStatement stmt = null;
        try {
            stmt = dbConnection
                    .prepareStatement("select id,imie,nazwisko,data_urodzenia from pracownik where nazwisko like ?");
            stmt.setString(1, surname);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                // tworzenie pracownika na podstawie biezacego rekordu
                Employee e = createEmployee(rs);
                employeeList.add(e);
                // tylko dla testu
               // System.out.println("Pracownik: " + e.getFirstName() + " "
                       // + e.getSurname() + " id: " + e.getId());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return employeeList;
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------  
    private static Employee createEmployee(ResultSet set){
    	Employee employee = new Employee();
    	try {
			employee.setId(set.getInt(1));
			employee.setFirstName(set.getString(2));
	    	employee.setSurname(set.getString("nazwisko"));
	        employee.setDateOfBirth(set.getDate("data_urodzenia"));
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	return employee;
    }
    
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
 // zwraca liste wszystkich pracownikow w bazie
    private static List<Employee> getEmployeeList(Connection dbConnection) {
        List<Employee> employeeList = new ArrayList<Employee>();

        PreparedStatement stmt = null;
        try {

            stmt = dbConnection.prepareStatement("select id,imie,nazwisko,data_urodzenia from pracownik");
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                Employee e = new Employee();
                // pobranie pol po numerze kolumny
                e.setId(rs.getInt(1));
                e.setFirstName(rs.getString(2));
                // pobranie pol po nazwie kolumny
                e.setSurname(rs.getString("nazwisko"));
                e.setDateOfBirth(rs.getDate("data_urodzenia"));
                employeeList.add(e);
                // tylko dla testu
//                System.out.println("Pracownik: " + e.getFirstName() + " "
//                        + e.getSurname() + " id: " + e.getId());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return employeeList;
    }
    
  //-------------------------------------------------------------------
  //-------------------------------------------------------------------
    
    private static void insertPensjaPracownik(Connection dbConnection, String firstname, String lastname, int id, int wynagrodzenie){
    	insertPensja(dbConnection, id, wynagrodzenie);
    	Statement s;
		try {
			s = dbConnection.createStatement();
			s.executeUpdate("update pensja set od='" + createDate() + "' where ID='" + id+"'");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		insertEmployee(dbConnection, firstname, lastname);
    	
    }
  //-------------------------------------------------------------------
  //-------------------------------------------------------------------
    private static void insertPensja(Connection dbConnection, int id, int wynagrodzenie){
    	Statement stmt = null;
    	try {
    		stmt = dbConnection.createStatement();
    		stmt.executeUpdate("insert into pensja(id, kwota)" + "values('" + id + "' , '" + wynagrodzenie + "')");

 
    	} catch(SQLException e){
    		e.printStackTrace();
    	} finally {
    		if (stmt != null){
    			try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
    		}
    	}
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
    private static void insertEmployee(Connection dbConnection, String firstName, String surname) {
        Statement stmt = null;
        try {
            stmt = dbConnection.createStatement();
            // uwaga - w tej wersji nie wstawiamy daty urodzenia 
            stmt.executeUpdate("insert into pracownik (imie,nazwisko) "
                    + "values( '" + firstName + "', '" + surname + "')");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------
    private static void insertEmployeeV2(Connection dbConnection, String firstName, String surname, Date dateOfBirth) {
        PreparedStatement stmt = null;
        try {
            stmt = dbConnection
                  .prepareStatement("insert into pracownik (imie,nazwisko,data_urodzenia) "
                            + "values(?,?,?)");
            stmt.setString(1, firstName);
            stmt.setString(2, surname);
            stmt.setDate(3, dateOfBirth);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // w kazdym wypadku jesli stmt nie null to go zamknij -
            // zwalnianie zasobow
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
    private static Date createDate() {	
        Calendar calendar = GregorianCalendar.getInstance();
        Date date = new Date(calendar.getTime().getTime());
        return date;
    } 
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------
    private static Date createDate(int year, int month, int day) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.clear();
        calendar.set(year, month, day);
        Date date = new Date(calendar.getTime().getTime());
        return date;
    }   
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
    static void clearPensja(Connection dbConnection, int n){
    	Statement s;
		try {
			s = dbConnection.createStatement();
	    	s.execute("delete from pensja where id>" + n);
		} catch (SQLException e) {		
			e.printStackTrace();
		}

    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------    
    static void clear(Connection dbConnection){
    	Statement s = null;
    	try {
    		s = dbConnection.createStatement();
    		s.execute("delete from pensja where id>0");
    		s.execute("delete from pracownik where id>0");
    	} catch (SQLException e){
    		e.printStackTrace();
    	} finally {
    		try {
				s.close();
			} catch (SQLException e){
				e.printStackTrace();
			}
    	}
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------   
    static void printEmployee(Employee e){
    	System.out.println(e.firstName + " " + e.surname + " ID: " + e.id);
    }
    //-------------------------------------------------------------------
    //-------------------------------------------------------------------
    static void printEmployeeList(Connection dbConnection){
    	 List<Employee> holdEmployeeList = getEmployeeList(dbConnection);
         for(Employee i: holdEmployeeList)
         	System.out.println(i.getFirstName() + " " + i.getSurname() + " " + i.getId() + " " + i.getDateOfBirth());
    }
    
}