package com.bigdatakinneret;

import java.io.IOException;
import java.util.*;
import tech.tablesaw.api.NumberColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.columns.numbers.Stats;

public class NotMapReduce {
	
	 int sumRainiest = 0;
	 int sumDriest = 20000;
	 String rYearDriest = "";
	 String rYearRainiest = "";
	 Table LoadInfo() throws IOException
	
	{

	//	Table t = Table.read().file("myFile.csv");
		
		 
		 
		 Table t1 = Table.read().csv("/home/y6x/eclipse-workspace/Finalproj/src/main/java/com/bigdatakinneret/DataFile_FinalExercise.csv");
			return t1;
				
	}
	
	public String[] MeanByYear(Table inTable)
	{
		List <String> myListNames = inTable.columnNames();
		int numHeader = inTable.columnNames().size() - 1;
		String[] meanByYear = new String[numHeader];
		Table myTb1 = inTable;
		for(int i=1; i < numHeader;i++) {
			meanByYear[i] = myListNames.get(i)+": " + String.valueOf(myTb1.nCol(i).mean());
		}
		return(meanByYear);
	}
	public void RainiestYearAndAmount(Table inTable)
	{
		int numHeader = inTable.columnNames().size();
		Table myTb1 = inTable;
		for(int i=1; i < numHeader;i++) {
			if(myTb1.nCol(i).sum() > this.sumRainiest)
			{
				this.sumRainiest = (int) myTb1.nCol(i).sum();
				this.rYearRainiest = myTb1.nCol(i).name();
			}
		}
		
	}
	public void DriestYearAndAmount(Table inTable)
	{
		int numHeader = inTable.columnNames().size();
		Table myTb1 = inTable;
		for(int i=1; i < numHeader;i++) {
			if(myTb1.nCol(i).sum() < this.sumDriest)
			{
				this.sumDriest = (int) myTb1.nCol(i).sum();
				this.rYearDriest = myTb1.nCol(i).name();
			}
		}
		
	}
	

	
	public int returnSumRainiest()
	{
		return this.sumRainiest;
	}
	public int returnSumDriest()
	{
		return this.sumDriest;
	}
	public String returnYearDriest()
	{
		return this.rYearDriest;
	}
	public String returnYearRainiest()
	{
		return this.rYearRainiest;
	}
	
	

}
