package com.ishan.smartcampus.hiveudf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class GradeConversion extends UDF{
	public Text evaluate(Text text)
	{
		double grade_adjustment = 0.0;
		String grade = text.toString();
		double final_grade = 0.0;
		if(grade == null)
		{
			return new Text("");
		}
		else if (grade.length()>1)
		{
			if(grade.charAt(1) == '+')
			{
				grade_adjustment = 0.3;
			}
			else if(grade.charAt(1) == '-')
			{
				grade_adjustment = -0.3;
			}
			
		}
		if(grade.contains("F") || grade.contains("IC") || grade.contains("WU") || grade.contains("NC"))
		{
			final_grade = 0.0;
		}
		else if(grade.contains("A") || grade.contains("CR"))
			{
			if(grade.contains("+"))
			{
				final_grade = 4.0;
			}
			else
			{
				final_grade = 4.0 + grade_adjustment;
			}
			}
		else if(grade.contains("B"))
			{
				final_grade = 3.0 + grade_adjustment;
			}
		else if(grade.contains("C"))
			{
				final_grade = 2.0 + grade_adjustment;
			}
		else if(grade.contains("D"))
			{
				final_grade = 1.0 + grade_adjustment;
			}
		
		
		return new Text(String.valueOf(final_grade));
	}

}
