# DreamAirlines
Spark training with CSV files from dreamAirlines

## Prerequisite
Before using the project be sure to have Spark installed and ready to be used. This link can help you to install spark on a ubuntu environment: 
https://computingforgeeks.com/how-to-install-apache-spark-on-ubuntu-debian/

You can find plenty of links for installing it on other OS.

## Setup

1. Clone the repository :
 
		git clone https://github.com/briceangiono/dreamAirlines.git

2. Go to the repository's root.


3. Run this:

		spark-shell -i scripts/dreamAirlinesDf.scala 

(This script put all the CSV files into dataframes ready to use.)

4. You can access the dataframes list at any moment typing : 

		print(dreamAirlines)

5. The goal of this is to train our spark skills with the sql exercices you can find in the SQL Training.pdf file present in the repository.

6. Enjoy it!

