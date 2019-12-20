val dfDimCar = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimCar.csv")
val dfDimCarRenting = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimCarRenting.csv")
val dfDimCustomer = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimCustomer.csv")
val dfDimDate = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimDate.csv")
val dfDimEmployee = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimEmployee.csv")
val dfDimHotel = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimHotel.csv")
val dfDimHotelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimHotelBooking.csv")
val dfDimInsurance = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimInsurance.csv")
val dfDimTravelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/dimTravelBooking.csv")
val dfFactCarRenting = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/factCarRenting.csv")
val dfFactHotelBooking = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/factHotelBooking.csv")
val dfFactInsurance = spark.read.option("inferSchema", true).option("header",true).csv("/home/bangiono/Documents/SparkCertif/DreamAirlines_DW/factInsurance.csv")

val dreamAirlines = List( "dfDimCar","dfDimCarRenting","dfDimCustomer","dfDimDate","dfDimEmployee","dfDimHotel","dfDimHotelBooking","dfDimInsurance","dfDimTravelBooking","dfFactCarRenting","dfFactHotelBooking","dfFactInsurance")

print(dreamAirlines)


