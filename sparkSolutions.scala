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

