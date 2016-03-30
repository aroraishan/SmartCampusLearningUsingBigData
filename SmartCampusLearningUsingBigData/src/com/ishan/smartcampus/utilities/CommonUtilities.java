package com.ishan.smartcampus.utilities;

public class CommonUtilities {
	
	public static String getTerm(String encodedTerm)
	{
		try
		{
			if(encodedTerm == null)
			{
				return "";
			}
			if(encodedTerm.length() == 1)
			{
				return "";
			}
		String graduation_date = encodedTerm.replace('"', ' ').trim();
		String Semester = "", year;
		
		switch(Integer.parseInt(graduation_date.substring(graduation_date.length()-1, graduation_date.length())))
		{
		case 1:
			Semester = "Winter";
			break;
		case 2:
			Semester = "Spring";
			break;
		case 3:
			Semester = "Summer";
			break;
		case 4:
			Semester = "Fall";
			break;
		}
		if(graduation_date.charAt(0) == '2')
		{
		year = "20" + graduation_date.substring(1,3); 
		return Semester + " " + year;
		}
		else if(graduation_date.charAt(0) == '9')
		{
			year = "19" + graduation_date.substring(0,2); 
			return Semester + " " + year;
		}
		else
		{
			return "";
		}
		}
		catch(Exception e)
		{
			System.out.println(encodedTerm);
			return "ishan";
		}
	}
	
	public static String removeBrackets(String unformattedString)
	{
		return unformattedString.replace('"', ' ').trim();
	}
	
	public static String getAge(String unformattedAge)
	{
		try
		{
		String birth_year = unformattedAge.split("/")[2];
		int age = 2016 - Integer.parseInt(birth_year);
		return Integer.toString(age);
		}
		catch(ArrayIndexOutOfBoundsException e)
		{
			return "0";
		}
		
		
	}

}
