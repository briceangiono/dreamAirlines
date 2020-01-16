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

// 18.
dfFactHotelBooking.
	join(dfDimHotelBooking, "PK_HotelBooking").
	where(upper(col("DSC_HotelService")).contains("BREAKFAST")).
	orderBy("AMT_Accomodation").
	limit(1).
	select("DSC_Hotel", "DSC_HotelService", "AMT_Accomodation").show

// 19.
import org.apache.spark.sql.expressions.window

val windowSpec = Window.partitionBy("DSC_RentalCompany")

dfDimCar.
	join(dfFactCarRenting, col("PK_Car") === col("FK_Car")).
	select("DSC_RentalCompany", "DSC_Car", "AMT_DailyRate").withColumn("Max_AMT_DailyRate", max(col("AMT_DailyRate")).over(windowSpec)).
	where(col("AMT_DailyRate") === col("Max_AMT_DailyRate")).
	drop("Max_AMT_DailyRate").
	distinct.show

// 20.
dfDimCar.
	join(dfFactCarRenting, col("PK_car") === col("FK_Car")).
	join(dfDimDate, col("FK_Date") === col("PK_Date")).
	filter(col("MMYYYY") === 72017).
	orderBy(col("AMT_Rental").desc).
	limit(1).
	select("DSC_Car", "AMT_Rental").show

// 21.
dfDimCar
	.join(dfFactCarRenting, col("PK_Car") === col("FK_Car"))
	.where(col("FK_Date").startsWith("2016"))
	.groupBy("DSC_Car")
		.agg(sum("AMT_CarInsurance").as("AMT_Insurance"))
	.filter(col("AMT_Insurance").lt(8000))
	.select(col("DSC_Car"), col("AMT_Insurance"))
	.orderBy(col("AMT_Insurance").desc, col("DSC_Car")).show

// 22.
dfDimTravelBooking
	.groupBy("DSC_Employee")
		.agg(countDistinct("PK_TravelBooking").alias("NbTravelBookings"))
	.orderBy(col("NbTravelBookings").desc)
	.limit(5).show

// 23.
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("DSC_TravelAgency").orderBy(count("PK_HotelBooking").desc)

dfDimHotelBooking
	.groupBy("DSC_TravelAgency", "DSC_Employee")
		.agg(
			count("PK_HotelBooking").as("NbBookings"), 
			rank().over(windowSpec).as("employeeRank"))
	.orderBy(col("DSC_TravelAgency"), col("NbBookings").desc)
	.filter(col("employeeRank").lt(4))
	.drop("employeeRank").show(false)

// 24.
import org.apache.spark.sql.expressions.Window

val windowTop = Window.partitionBy("DSC_TravelAgency").orderBy(count("PK_HotelBooking").desc)

val windowBottom = Window.partitionBy("DSC_TravelAgency").orderBy(count("PK_HotelBooking"))

dfDimHotelBooking
	.groupBy("DSC_TravelAgency", "DSC_Employee")
		.agg(
			count("PK_HotelBooking").as("NbBookings"), 
			rank().over(windowTop).as("topRank"), 
			rank().over(windowBottom).as("bottomRank"))
	.orderBy(col("DSC_TravelAgency"), col("NbBookings").desc)
	.filter(col("topRank").leq(3) || col("bottomRank").leq(3))
	.drop("topRank", "bottomRank").show(false)

// 25.
dfFactInsurance
	.join(dfDimInsurance, "PK_Insurance")
	.join(dfDimEmployee, col("FK_Employee") === col("PK_Employee"))
	.filter(!upper(col("DSC_Type")).contains("STANDARD") && col("AMT_Travel_Insurance") =!= 0.0)
	.groupBy("DSC_Employee")
		.agg(round(sum(col("AMT_Travel_Insurance")), 2).as("Amt_Insurance"))
	.orderBy(col("Amt_Insurance").desc).show

// 26.
dfDimEmployee
	.select(col("DSC_Employee"), col("DSC_JobTitle"), floor(months_between(current_date(), to_date(col("Contract_Date").cast(StringType), "yyyyMMdd")).divide(12)).as("Seniority"))
	.orderBy(col("Seniority").desc).show

// 27.
dfFactTravelBooking
	.withColumn("Year", year(to_date(col("FK_Date").cast(StringType), "yyyyMMdd")))
	.withColumn("Month", month(to_date(col("FK_Date").cast(StringType), "yyyyMMdd")))
	.groupBy("Year", "Month")
		.agg(sum("AMT_Travel").as("AMT_Travel"))
	.orderBy(col("Year"), col("Month")).show

// 28.
dfDimEmployee
	.join(dfFactTravelBooking, col("FK_Employee") === col("PK_Employee"))
	.withColumn("Year", year(to_date(col("FK_date").cast(StringType), "yyyyMMdd")))
	.filter(col("Year") === lit(2016))
	.groupBy("DSC_Employee", "Year")
		.agg(round(avg("AMT_Travel"), 2).as("AVG_Amt_Travel"))
	.orderBy(col("AVG_Amt_Travel").desc).show

// 29.
dfFactTravelBooking
	.groupBy("FK_Employee")
		.agg(sum("AMT_Travel").as("SUM_Amt_Travel"))
	.join(dfDimEmployee, col("FK_Employee") === col("PK_Employee"))
	.withColumn("Total", sum(col("SUM_Amt_Travel")).over())
	.where(floor(months_between(current_date(), to_date(col("Contract_Date").cast(StringType), "yyyyMMdd")).divide(12)).gt(5))
	.withColumn("PercentTotal", round(col("SUM_Amt_Travel") / col("Total") * 100, 2))
	.select("DSC_Employee", "PercentTotal")
	.orderBy(col("PercentTotal").desc).show

// 30.
dfDimCar.withColumn("DSC_Color", lit(null)).show

// 31.
val dfCarColors = List(
   ("Nissan Micra 1.0" ,"White"),
   ("Volkswagen UP 1.0", "Black"),
   ("Peugeot 108 1.0", "White"),
   ("Smart Forfour 1.1", "Black"),
   ("Renault Twingo 1.2", "White"),
   ("Volkswagen Polo 1.2", "Black"),
   ("Opel Adam 1.4 D", "White"),
   ("Opel Corsa 1.4 D", "Black"),
   ("Seat Ibiza 1.4 TDI", "Red"),
   ("Renault Clio 1.5 DCI", "Black"),
   ("Ford Fiesta 1.5", "White"),
   ("Audi A3 1.6", "Black"),
   ("BMW 116i 1.6", "Red"),
   ("Mercedes Benz C Class 1.5", "White"),
   ("Volkswagen Passat 2.0", "Grey"),
   ("BMW 320i Sport 2.0 Turbo", "Blue"),
   ("Mercedes-Benz CLS 500 5.0", "White"),
   ("Ford Mustang GT", "Green"),
   ("Ferrari 488 GTB", "Red"),
   ("Lamborghini Aventador", "Black"),
   ("Renault 5 GT Turbo", "Yellow"),
   ("Opel Corsa A 1.5 D", "Red"),
   ("Volkswagen Lupo 1.0", "Green"),
   ("Fiat Panda 1.2", "Grey"),
   ("Skoda Fabia 1.6", "Blue")
).toDF("DSC_Car","DSC_CarColor")

dfDimCar.join(dfCarColors, Seq("DSC_Car"), "left").show

// 32.
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.monotonically_increasing_id

val motoSchema = StructType(
	StructField("PK_Moto", IntegerType, false),
	StructField("DSC_RentalCompany", StringType, true),
	StructField("DSC_Moto", StringType, true),
	StructField("Load_ID", IntegerType, true),
	StructField("HashColumn", StringType, true),
	StructField("Insert_Date", DateType, true),
	StructField("Update_Date", DateType, true)
)

val dfMotos = List(("GlobalMotors", "Harley-Davidson Street 500"),("GlobalMotors", "Harley-Davidson Street 700"),("GlobalMotors", "Harley-Davidson Sportster Iron 883"),("GlobalMotors", "Harley-Davidson V-Rod Night"),("GlobalMotors", "Harley-Davidson Dyna Fat Bob")).toDF("DSC_RentalCompany", "DSC_Moto")

val reorderedColumnNames = List("PK_Moto", "DSC_RentalCompany", "DSC_Moto", "Load_ID", "HashColumn", "Insert_Date", "Update_Date")

val dfDimMoto = dfMotos
					.withColumn("PK_Moto", row_number().over(Window.orderBy(monotonically_increasing_id())))
					.withColumn("Load_ID", lit(1)).withColumn("HashColumn", lit(null)).withColumn("Insert_Date", current_date())
					.withColumn("Update_Date", current_date())
					.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)

// The monotonically_increasing_id() isn't necessary
val dfDimMotoBis = dfMotos
					.withColumn("PK_Moto", row_number().over(Window.orderBy(lit(1)))).withColumn("Load_ID", lit(1))
					.withColumn("HashColumn", lit(null)).withColumn("Insert_Date", current_date())
					.withColumn("Update_Date", current_date())
					.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)


// 33.
import org.apache.spark.sql.types._

val maxPk = dfDimCar.agg(max("PK_Car").as("maxPk")).first.getInt(0)

val dfDimMoto = dfDimMoto.withColumn("PK_Moto", col("PK_Moto") + maxPk)

val dfDimVehicule = dfDimCar
						.union(dfDimMoto)
						.withColumnRenamed("PK_Car", "PK_Vehicule")
						.withColumnRenamed("DSC_Car", "DSC_Vehicule")

dfDimVehicule.coalesce(1).write.format("csv").option("header", "true").save("./data/dreamAirlines_DW/dfDimVehicule")
