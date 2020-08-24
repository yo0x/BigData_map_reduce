package com.bigdatakinneret;

import java.io.IOException;


import tech.tablesaw.api.QuerySupport;
import tech.tablesaw.api.Table;

public class AppBigData {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
	   
		
		NotMapReduce myNmr = new NotMapReduce();
		Table t2 = myNmr.LoadInfo();
		String[] meansByYear = myNmr.MeanByYear(t2);
		System.out.print("BigData final project \n");
		System.out.print("Section 1: \n Means by year: \n");
		for (int i=1; i< t2.columnNames().size()-1; i++)
		{
			System.out.println(meansByYear[i]);
		}
		myNmr.RainiestYearAndAmount(t2);
		System.out.print("Section 2: " + " Rainiest year: "+myNmr.returnYearRainiest()  +  ". Amount: " + myNmr.returnSumRainiest()+"\n");
//		System.out.print(myNmr.RainiestYearAndAmount(t2)+"\n");
		Table sec3 = t2.where(t2.numberColumn("1927").isEqualTo(t2.nCol("1927").max()));
		Table sec4 = t2.where(t2.numberColumn("1990").isEqualTo(t2.nCol("1990").min()));

	//	System.out.print(sec3);
		System.out.println("Section 3: Rainiest year is " + sec3.nCol("1927").name() + ", the rainiest month is "+sec3.stringColumn("Month").get(0) + " with amount:" + sec3.nCol("1927").getDouble(0));
		myNmr.DriestYearAndAmount(t2);
		System.out.print("Section 4: " + " Driest year: "+myNmr.returnYearDriest()  +  ". Amount: " + myNmr.returnSumDriest()+"\n");
		
		System.out.println("Section 5: Driest year is " + sec4.nCol("1990").name() + ", the Driest month is "+sec4.stringColumn("Month").get(0) + " with amount:" + sec4.nCol("1990").getDouble(0));
		
		//Section 6.
		Table tWinter = t2.where(t2.stringColumn("Month").isEqualTo("December").or(t2.stringColumn("Month").isEqualTo("January")).or(t2.stringColumn("Month").isEqualTo("February")));
		//System.out.print(tWinter);
		Table tSummer = t2.where(t2.stringColumn("Month").isEqualTo("June").or(t2.stringColumn("Month").isEqualTo("July")).or(t2.stringColumn("Month").isEqualTo("August")));
		//System.out.print(tSummer);
		Table tSpring = t2.where(t2.stringColumn("Month").isEqualTo("March").or(t2.stringColumn("Month").isEqualTo("April")).or(t2.stringColumn("Month").isEqualTo("May")));
		//System.out.print(tSpring);
		Table tFall = t2.where(t2.stringColumn("Month").isEqualTo("September").or(t2.stringColumn("Month").isEqualTo("October")).or(t2.stringColumn("Month").isEqualTo("December")));
		//System.out.print(tFall);
		int seassonLargestAmount = 0;
		int seassonSmallesttAmount = 40000;
		

		//--Winter: Calculate Rainiest, Driest year with their amount.
		myNmr.RainiestYearAndAmount(tWinter);
		String WinterRainiestYear = myNmr.returnYearRainiest();
		int WinterRainiestAmount = myNmr.returnSumRainiest();
		myNmr.DriestYearAndAmount(tWinter);
		String WinterDriestYear = myNmr.returnYearDriest();
		int WinterDriestAmount = myNmr.returnSumDriest();
		//---Fall: Calculate Rainiest, Driest year with their amount.
		myNmr.RainiestYearAndAmount(tFall);
		String FallRainiestYear = myNmr.returnYearRainiest();
		int FallRainiestAmount = myNmr.returnSumRainiest();
		myNmr.DriestYearAndAmount(tFall);
		String FallDriestYear = myNmr.returnYearDriest();
		int FallDriestAmount = myNmr.returnSumDriest();
		//--Summer: Calculate Rainiest, Driest year with their amount.
		myNmr.RainiestYearAndAmount(tSummer);
		String SummerRainiestYear = myNmr.returnYearRainiest();
		int SummerRainiestAmount = myNmr.returnSumRainiest();
		myNmr.DriestYearAndAmount(tSummer);
		String SummerDriestYear = myNmr.returnYearDriest();
		int SummerDriestAmount = myNmr.returnSumDriest();
		//--Spring: Calculate Rainiest, Driest year with their amount.
		myNmr.RainiestYearAndAmount(tSpring);
		String SpringRainiestYear = myNmr.returnYearRainiest();
		int SpringRainiestAmount = myNmr.returnSumRainiest();
		myNmr.DriestYearAndAmount(tSpring);
		String SpringDriestYear = myNmr.returnYearDriest();
		int SpringDriestAmount = myNmr.returnSumDriest();

		int[] AmountsRainiest = {WinterRainiestAmount,FallRainiestAmount,SummerRainiestAmount,SpringRainiestAmount};
		int[] AmountsDriest = {WinterDriestAmount,FallDriestAmount,SummerDriestAmount,SpringDriestAmount};
		System.out.print("Section 6: ");
		for (int i = 0; i<3; i++)
		{
			if(AmountsRainiest[i]>seassonLargestAmount)
			{
				seassonLargestAmount = AmountsRainiest[i];
			}
			if(AmountsDriest[i]<seassonSmallesttAmount)
			{
				seassonSmallesttAmount = AmountsDriest[i];
			}
			
		}
		
		if(seassonLargestAmount==WinterRainiestAmount)
		{
			System.out.print("Rainiest seasson is: Winter" + " the year "+ WinterRainiestYear + " with amount: "+ seassonLargestAmount+". \n");
		}else if(seassonLargestAmount==FallRainiestAmount)
		{
			System.out.print("Rainiest seasson is: Fall" + " the year "+ FallRainiestYear + " with amount: "+ seassonLargestAmount+". \n");

		}
		else if(seassonLargestAmount==SummerRainiestAmount)
		{
			System.out.print("Rainiest seasson is: Summer" + " the year "+ SummerRainiestYear + " with amount: "+ seassonLargestAmount+". \n");

		}
		else if(seassonLargestAmount==SpringRainiestAmount)
		{
			System.out.print("Rainiest seasson is: Spring" + " the year "+ SpringRainiestYear + " with amount: "+ seassonLargestAmount+". \n");

		}
		//section 7
		System.out.print("Section 7: ");

		if(seassonSmallesttAmount==WinterDriestAmount)
		{
			System.out.print("Driest seasson is: Winter" + " the year "+ WinterDriestYear + " with amount: "+ seassonSmallesttAmount+". \n");
		}else if(seassonSmallesttAmount==FallDriestAmount)
		{
			System.out.print("Driest seasson is: Fall" + " the year "+ FallDriestYear + " with amount: "+ seassonSmallesttAmount+". \n");

		}
		else if(seassonSmallesttAmount==SummerDriestAmount)
		{
			System.out.print("Driest seasson is: Summer" + " the year "+ SummerDriestYear + " with amount: "+ seassonSmallesttAmount+". \n");

		}
		else if(seassonSmallesttAmount==SpringDriestAmount)
		{
			System.out.print("Driest seasson is: Spring" + " the year "+ SpringDriestYear + " with amount: "+ seassonSmallesttAmount+". \n");

		}
		
		

		
		
	}

}
