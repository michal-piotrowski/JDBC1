package pl.agh.edu.kis.JDBC;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Main {
	public static void main(String[] args) {
		Calendar calendar = new GregorianCalendar();
		Date date = new Date(calendar.getTime().getTime());
		System.out.println(date.toString());
	}
	
}
