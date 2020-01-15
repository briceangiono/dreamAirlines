// ex2
dfDimHotelBooking.selectExpr("count(distinct(DSC_Employee)) as Cnt_Rows").show()

// ex 3
dfDimCustomer.selectExpr("DSC_Customer").where($"DSC_Customer".contains("A")).show()
dfDimCustomer.selectExpr("DSC_Customer").where($"DSC_Customer".like("A%")).show()

// ex 4

dfDimTravelBooking.selectExpr("(DSC_TravelDestiny)").distinct().where(not($"DSC_TravelDestiny".like("%a"))).show()

// ex5

dfDimHotelBooking.selectExpr("upper(substring(DSC_TravelDestiny,1,2)) as COD_TravelDestiny").show()

dfDimHotelBooking.select(upper(expr("substring(DSC_TravelDestiny,1,2)"))).withColumnRenamed("upper(substring(DSC_TravelDestiny, 1, 2))","COD_TravelDestiny").show()
dfDimHotelBooking.select((upper(expr("substring(DSC_TravelDestiny,1,2)"))).as("COD_TravelDestiny")).show()

//ex6
dfDimTravelBooking.where($"DSC_TravelDestiny" ==="Brazil").count()

//ex7
dfDimTravelBooking.groupBy(col("DSC_Employee"),col("DSC_TravelAgency")).count.show()
dfDimTravelBooking.cube(col("DSC_Employee"),col("DSC_TravelAgency")).count.show()

//ex8 ??? type date ?
dfDimEmployee.select(col("DSC_Employee"),col("Insert_Date")).show(5)

//ex 9
dfDimEmployee.withColumn("split_tab", split(col("DSC_Employee")," "))
                .withColumn("First Name", col("split_tab")
                .getItem(0)).withColumn("Last Name",col("split_tab")
                .getItem(1)).drop("split_tab")
                .show(5)

// 10
dfDimHotel.select(concat(col("DSC_Hotel"),lit(" "),col("DSC_HotelCountry")).as("HotelName_Country")).show()

//11

dfDimCustomer.withColumn("Birthdate", to_date(col("Birthdate").cast(StringType), "yyyyMMdd")).show()
import org.apache.spark.sql.types.TimestampType

val dfDimCustomer_date = dfDimCustomer.withColumn("Birthdate", unix_timestamp(to_date(col("Birthdate").cast(StringType), "yyyyMMdd"), "yyyy/MM/dd HH:mm:ss").cast(TimestampType))

//12
dfDimCustomer_date.select(round(months_between(current_date(),col("Birthdate")).divide(12)).as("Age"), expr("*")).orderBy(desc("Age")).show()
val dfDimCustomer_age = dfDimCustomer_date.select(round(months_between(current_date(),col("Birthdate")).divide(12)).as("Age"), expr("*")).orderBy(asc("Age"))

//13
dfDimCustomer_age.select("Age").withColumn("SegmentAge", when(col("Age") <=17 , "A").when(col("Age") <=24 , "B").when(col("Age") <=34 , "C").when(col("Age") <=44 , "D").when(col("Age") <=54 , "E").when(col("Age") <=64 , "F").otherwise("G")).groupBy(col("SegmentAge")).count.orderBy("SegmentAge").show()

dfDimCustomer_age.select("Age").withColumn("SegmentAge", when(col("Age") <=17 , "A").when(col("Age") <=24 , "B").when(col("Age") <=34 , "C").when(col("Age") <=44 , "D").when(col("Age") <=54 , "E").when(col("Age") <=64 , "F").otherwise("G")).groupBy(col("SegmentAge")).agg(max("Age").alias("max")).orderBy("SegmentAge").show()

// 14
dfDimCustomer_age.select("Age").agg(max("Age").alias("max")).show()

// 15
dfDimCar.join(dfDimCarRenting, col("FK_Car")=== col("PK_Car"), "left_anti").select(col("PK_Car"),col("DSC_Car")).show()

//16

dfDimEmployee.select("Load_ID").agg(max("Load_ID").as("MaxSalary")).show()
//17
dfDimEmployee.select("Load_ID").agg(min("Load_ID").as("MinSalary")).show()

//18

dfFactHotelBooking.withColumnRenamed("PK_HotelBooking","PK_HotelBooking_fact").withColumnRenamed("FK_TravelBooking","FK_TravelBooking_fact")
    .join(dfDimHotelBooking, col("FK_TravelBooking") === col("FK_TravelBooking_fact"))
    .where(col("DSC_HotelService")==="Breakfast")
    .groupBy("DSC_Hotel").agg(min(col("AMT_Accomodation")).as("MinAmt"),max(col("AMT_Accomodation")).as("MaxAmt"))
    .orderBy(asc("MinAmt")).show(1)
//19

dfFactCarRenting.show(5)
dfDimCar.show(5)

dfFactCarRenting.join(dfDimCar, col("PK_Car") === col("FK_Car")).select(col("DSC_RentalCompany"), col("DSC_Car"),col("AMT_DailyRate")).groupBy(col("DSC_RentalCompany"), col("DSC_Car")).agg(mean(col("AMT_DailyRate")).as("AMT_DailyRate")).show()

//20
dfFactCarRenting.join(dfDimCar, col("PK_Car") === col("FK_Car")).where(col("FK_Date").like("201707%")).select( col("DSC_Car"),col("AMT_Rental"),col("FK_Date")).orderBy(desc("AMT_Rental")).show(1)

//21
dfFactCarRenting.join(dfDimCar, col("PK_Car") === col("FK_Car")).where(col("FK_Date").like("2016%")).groupBy(col("DSC_Car")).agg(sum(col("AMT_CarInsurance")).as("AMT_Insurance")).where(col("AMT_Insurance")<8000).orderBy(desc("AMT_Insurance")).show()


//22
dfDimTravelBooking.groupBy("DSC_TravelStatus").count.show()
dfDimTravelBooking.show(5)

dfDimTravelBooking.where(col("DSC_TravelStatus")==="Confirmed").groupBy(col("DSC_Employee")).agg(count("DSC_Employee").as("NbTravelBookings")).orderBy(desc("NbTravelBookings")).show(5)

//23

dfDimHotelBooking.groupBy(col("DSC_TravelAgency"),col("DSC_Employee")).agg(count("*").as("NbBookings")).orderBy(desc("NbBookings")).show(3)

//24
val dfDimHotelBooking_with_nbBooking =  dfDimHotelBooking.groupBy(col("DSC_TravelAgency"),col("DSC_Employee")).agg(count("*").as("NbBookings")).orderBy(desc("NbBookings"))
val dfTop3Booking = dfDimHotelBooking_with_nbBooking.limit(3)
val dfBot3Booking = dfDimHotelBooking_with_nbBooking.orderBy(asc("NbBookings")).limit(3)
dfTop3Booking.union(dfBot3Booking).show()

//25
dfFactInsurance.show(5)
dfDimEmployee.show(5)


val dfFactInsClean = dfFactInsurance
                        .where(col("AMT_Travel_Insurance") > 0)
                        .join(dfDimInsurance.select(col("PK_Insurance").as("PK_Insurance_dim"),col("DSC_Insurance")),col("PK_Insurance") === col("PK_Insurance_dim") )
                        .where(col("DSC_Insurance") =!= "Standard")

dfDimEmployee.join(dfFactInsClean, col("PK_Employee") === col("FK_Employee"))
                .groupBy(col("DSC_Employee"))
                .agg(round(sum("AMT_Travel_Insurance"),2).as("Amt_Insurance"))
                .orderBy(desc("Amt_Insurance")).show()

//26


dfDimEmployee.select(col("DSC_Employee"), 
                    col("DSC_JobTitle"), 
                    col("Amt_Salary"), 
                    round(months_between(current_date, to_date(col("Contract_Date").cast(StringType),"yyyyMMdd")).divide(12)).as("Seniority")
                ).orderBy(desc("Seniority")).show()

//27 replace FactTravel vy FactBooking
import org.apache.spark.sql.types.StringType

dfFactHotelBooking.withColumn("FK_Date", to_date(col("FK_Date").cast(StringType),"yyyyMMdd")).groupBy(year(col("FK_Date")).as("Year"),month(col("FK_Date")).as("Month")).agg(sum("AMT_Accomodation").as("Amt_Booking")).orderBy(asc("Year"), asc("Month")) .show()


//28

dfFactHotelBooking.withColumn("Year", year(to_date(col("FK_Date").cast(StringType),"yyyyMMdd"))).where(col("Year")===2016).join(dfDimEmployee, col("PK_Employee") === col("FK_Employee")).groupBy(col("DSC_Employee"), col("Year")).agg(round(mean(col("AMT_Accomodation")),2).as("AVG_AMT_Accomodation")).orderBy(desc("AVG_AMT_Accomodation")).show()

//29

val dfDimEmployee_with_exp = dfDimEmployee.withColumn("Seniority", round(months_between(current_date, to_date(col("Contract_Date").cast(StringType),"yyyyMMdd")).divide(12))).where(col("Seniority") > 5)
val totalAMT_Accomodation = dfFactHotelBooking.agg(sum(col("AMT_Accomodation"))).first.get(0)
dfFactHotelBooking.join(dfDimEmployee, col("PK_Employee") === col("FK_Employee")).groupBy(col("DSC_Employee")).agg(round(sum("AMT_Accomodation")*100/totalAMT_Accomodation,2).as("PercentTotal")).orderBy(desc("PercentTotal")).show

//30 & 31
val white_list = List("Nissan Micra 1.0","Peugeot 108 1.0","Opel Adam 1.4 D","Ford Fiesta 1.5", "Mercedes Benz C Class 1.5", "Mercedes-Benz CLS 500 5.0")
val blue_list = List("BMW 320i Sport 2.0 Turbo", "Skoda Fabia 1.6")
val black_list = List("Volkswagen UP 1.0", "Smart Forfour 1.1", "Volkswagen Polo 1.2", "Opel Corsa 1.4 D", "Renault Clio 1.5 DCI","Audi A3 1.6","Lamborghini Aventador")
val red_list = List("Opel Corsa A 1.5 D", "Ferrari 488 GTB", "BMW 116i 1.6", "Seat Ibiza 1.4 TDI")
val grey_list = List("Volkswagen Passat 2.0")
val green_list = List("Ford Mustang GT","Volkswagen Lupo 1.0")
val yellow_list = List("Renault 5 GT Turbo")
dfDimCar.withColumn("DSC_CarColor", when(col("DSC_Car").isin(white_list:_*)  , "white").when(col("DSC_Car").isin(blue_list:_*)  , "Blue").when(col("DSC_Car").isin(grey_list:_*)  , "Grey").when(col("DSC_Car").isin(green_list:_*)  , "green").when(col("DSC_Car").isin(red_list:_*)  , "Red").when(col("DSC_Car").isin(yellow_list:_*)  , "Yellow").when(col("DSC_Car").isin(black_list:_*)  , "Black").otherwise("Other")).groupBy(col("DSC_CarColor")).count.show()

dfDimCar