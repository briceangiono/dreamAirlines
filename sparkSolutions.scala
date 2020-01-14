// SQL Training
// 1. 
dfDimHotelBooking.selectExpr("count(*) AS Cnt_Rows").show

// 2.
dfDimEmployee.distinct.count

// 3.
dfDimCustomer.select("DSC_Customer").filter(col("DSC_Customer").startsWith("A")).show

// 4.
dfDimTravelBooking.select("DSC_TravelDestiny").distinct.filter(!upper(col("DSC_TravelDestiny")).endsWith("A")).show

// 5.
dfDimHotelBooking.selectExpr("*", "upper(left(DSC_TravelDestiny, 2)) AS COD_TravelDestiny").count
// Intresting thing to try is to reorder the columns in the same way as the screenshot

// 6.
dfDimHotelBooking.where(col("DSC_TravelDestiny") === "Brazil").show

// 7.
dfDimTravelBooking.select("DSC_Employee","DSC_TravelAgency").orderBy("DSC_TravelAgency").distinct.show(false)

// 8.
dfDimEmployee.select(col("DSC_Employee"), 
					round(months_between(lit(current_date()), to_date(col("Contract_Date").cast(StringType), "yyyymmdd")).divide(12)).as("Antiquity_Company")).
				orderBy(desc("Antiquity_Company")).show

// 9.
dfDimEmployee.withColumn("EmployeeName", split(col("DSC_Employee"), " ")).
				withColumn("FirstName", col("EmployeeName").getItem(0)).
				withColumn("LastName", col("EmployeeName").getItem(1)).
				drop("EmployeeName", "DSC_Employee").show

// 10.
dfDimHotel.withColumn("HotelName_Country", concat(col("DSC_Hotel"), 
													lit(" ("), 
													col("DSC_HotelCountry"), 
													lit(")"))).show
// 11.
dfDimCustomer.withColumn("Birthdate", to_timestamp(to_date(expr("cast(Birthdate as String)"), "yyyyMMdd"))).show

// 12.
dfDimCustomer.withColumn("Age", round(months_between( lit(current_date()), to_date(expr("cast(Birthdate as String)"), "yyyyMMdd") ).divide(12))).orderBy("Age").show

// 13.
val dfCustomerAge = dfDimCustomer.withColumn("Age", round(months_between( lit(current_date()), to_date(expr("cast(Birthdate as String)"), "yyyyMMdd") ).divide(12))).orderBy("Age")
dfCustomerAge.select(
	when(col("Age").between(10, 17), "Segment A").
	when(col("Age").between(18, 24), "Segment B").
	when(col("Age").between(25, 34), "Segment C").
	when(col("Age").between(35, 44), "Segment D").
	when(col("Age").between(45, 54), "Segment E").
	when(col("Age").between(55, 64), "Segment F").
	when(col("Age") >= 65, "Segment G").alias("SegmentAge")).
groupBy("SegmentAge").
agg(count("SegmentAge").alias("Amt_Customers")).
orderBy(desc("Amt_Customers")).show


// Extra
dfCustomerAge.agg(
	count(when(col("Age").between(10, 17), "Segment A")).alias("Segment A"),
	count(when(col("Age").between(18, 24), "Segment B")).alias("Segment B"),
	count(when(col("Age").between(25, 34), "Segment C")).alias("Segment C"),
	count(when(col("Age").between(35, 44), "Segment D")).alias("Segment D"),
	count(when(col("Age").between(45, 54), "Segment E")).alias("Segment E"),
	count(when(col("Age").between(55, 64), "Segment F")).alias("Segment F"),
	count(when(col("Age") >= 65, "Segment G")).alias("Segment G")).
	pivot()
	.show

// 14.
val dfEmployeeAge = dfDimEmployee.withColumn("Age", round(months_between( lit(current_date()), to_date(expr("cast(Birthdate as String)"), "yyyyMMdd") ).divide(12))).orderBy("Age")
dfEmployeeAge.select("DSC_Employee", "Age").orderBy(desc("Age")).limit(1).show

// 15.
dfDimCar.as("c").join(dfDimCarRenting.as("r"), col("c.PK_Car") === col("r.FK_Car"), "left").
				where(col("r.PK_CarRenting").isNull).
				select("PK_CarRenting", "DSC_Car").show

// 16.
dfDimEmployee.select(max("Amt_Salary").alias("Highest_Salary")).show

// 17.
dfDimEmployee.orderBy("Amt_Salary").select(col("Amt_Salary").as("Lowest_Salary")).distinct.limit(1).show
// or
dfDimEmployee.select(min("Amt_Salary").as("Lowest_Salary")).show
// or
dfDimEmployee.agg(min("Amt_Salary").as("Lowest_Salary")).show

//18.
