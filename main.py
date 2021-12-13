# Brian D. Christman
# Purpose: To read in, clean, and aggregate three files into a SQL database.
# The user is able to confirm their data are valid using this program prior to exploring and using the database in SQL.

# Import packages used in this program
import numpy as np
import collections
import pymysql.cursors

# Import data from files.py
from creds import *
from files import *

# Clean the housing data file
# Drop all rows containing a corrupt guid and save the remaining position indices
print("Beginning import")
print("Cleaning Housing File data")
housingIndices = []
for i in range(0,100):
    if len(housingFile.guid[i]) <= 4:
        housingIndices.append(i)
housingFile1 = housingFile.drop(housingFile.index[housingIndices])
housingIndices = housingFile1.index

# Remove the hyphens from each instance in the the guid variable
for i in housingIndices:
    housingFile1.guid[i] = housingFile1.guid[i].replace('-', '')

# Replace corrupt housing median age instances with a random integer between 10 and 50
for i in housingIndices:
    if len(housingFile1.housing_median_age[i]):
        housingFile1.housing_median_age[i] = int(np.random.randint(10,51,size=1))

# Replace corrupt total rooms instances with a random integer between 1000 and 2000
for i in housingIndices:
    if not housingFile1.total_rooms[i].isnumeric():
        housingFile1.total_rooms[i] = int(np.random.randint(1000,2001,size=1))

# Replace corrupt total bedrooms instances with a random integer between 1000 and 2000
for i in housingIndices:
    if not housingFile1.total_bedrooms[i].isnumeric():
        housingFile1.total_bedrooms[i] = int(np.random.randint(1000, 2001, size=1))

# Replace corrupt population instances with a random integer between 5000 and 10000
for i in housingIndices:
    if not housingFile1.population[i].isnumeric():
        housingFile1.population[i] = int(np.random.randint(5000, 10001, size=1))

# Replace corrupt households instances with a random integer between 500 and 2500
for i in housingIndices:
    if not housingFile1.households[i].isnumeric():
        housingFile1.households[i] = int(np.random.randint(500, 2501, size=1))

# Replace corrupt housing median house value instances with a random integer between 100000 and 250001
for i in housingIndices:
    if not housingFile1.median_house_value[i].isnumeric():
        housingFile1.median_house_value[i] = int(np.random.randint(100000, 250001, size=1))

# Remove the ZIP code variable from the cleaned housing file and print how many records will be entered in the database
housingFile1.drop(housingFile1.columns[[1]], axis=1, inplace=True)
print(f"{len(housingIndices)} records imported into the database")

# Clean the Income File data
# Drop all rows containing a corrupt guid and save the remaining position indices
print("Cleaning Income File data")
incomeIndices = []
for i in range(0,100):
    if len(incomeFile.guid[i]) <= 4:
        incomeIndices.append(i)
incomeFile1 = incomeFile.drop(incomeFile.index[incomeIndices])
incomeIndices = incomeFile1.index

# Remove the hyphens from each instance in the the guid variable
for i in incomeIndices:
    incomeFile1.guid[i] = incomeFile1.guid[i].replace('-', '')

# Replace corrupt median income instances with a random integer between 100000 and 750000
for i in incomeIndices:
    if not incomeFile1.median_income[i].isnumeric():
        incomeFile1.median_income[i] = int(np.random.randint(100000, 750001, size=1))

# Remove the ZIP code variable from the cleaned income file and print how many records will be entered in the database
incomeFile1.drop(incomeFile1.columns[[1]], axis=1, inplace=True)
print(f"{len(incomeIndices)} records imported into the database")

# Clean the ZIP File data
# Drop all rows containing a corrupt guid and save the remaining position indices
print("Cleaning ZIP File data")
zipIndices = []
for i in range(0,100):
    if len(zipFile.guid[i]) <= 4:
        zipIndices.append(i)
zipFile1 = zipFile.drop(zipFile.index[zipIndices])
zipIndices = zipFile1.index

# Remove the hyphens from each instance in the the guid variable
for i in zipIndices:
    zipFile1.guid[i] = zipFile1.guid[i].replace('-', '')

# Identify the cities that have duplicates amongst the records (will be used for conditional testing)
repeat_cities = [item for item, count in collections.Counter(zipFile1.city).items() if count > 1]

# Replace the corrupt ZIP code instances with modified ZIP codes from same city or county/state
for i in zipIndices:
    if not zipFile1.zip_code[i].isnumeric() and zipFile1.city[i] not in repeat_cities:
        for j in zipIndices:
            if i != j and zipFile1.county[i] == zipFile1.county[j] and zipFile1.state[i] == zipFile1.state[j]:
                firstCharacter = zipFile1.zip_code[j][:1]
                zipFile1.zip_code[i] = firstCharacter + "0000"
                break
    elif not zipFile1.zip_code[i].isnumeric():
        for j in zipIndices:
            if i != j and zipFile1.city[i] == zipFile1.city[j] and zipFile1.state[i] == zipFile1.state[j]:
                firstCharacter = zipFile1.zip_code[j][:1]
                zipFile1.zip_code[i] = firstCharacter + "0000"
                break

# Print how many records will be entered in the database
print(f"{len(zipIndices)} records imported into the database")

# Merge the three datasets into one file based on guid variable and store the record indices for the full dataframe
mergedataFile1 = housingFile1.merge(incomeFile1, on='guid', how='left')
data = mergedataFile1.merge(zipFile1, on='guid', how='left')
dataIndices = data.guid.index

# Convert all numeric variables into integers
data['housing_median_age'] = data['housing_median_age'].astype(int)
data['total_rooms'] = data['total_rooms'].astype(int)
data['total_bedrooms'] = data['total_bedrooms'].astype(int)
data['population'] = data['population'].astype(int)
data['households'] = data['households'].astype(int)
data['median_house_value'] = data['median_house_value'].astype(int)
data['median_income'] = data['median_income'].astype(int)
data['zip_code'] = data['zip_code'].astype(int)
print("Import completed \n")

# Connect to the database
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)

except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
    exit()

# Now that we are connected, execute a query
#  and do something with the result set.
try:
    with myConnection.cursor() as cursor:
        # ==================

        # NOTE: We are using placeholders in our SQL statements
        #  See https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html

        # Create a command to clear the housing table
        sqlDelete = """
          DROP TABLE housing;
        """

        # Create a command to generate a new housing table
        sqlCreate = """
        CREATE TABLE housing
               (
                `id`                 int not null auto_increment primary key,
                `guid`               char(32) not null,
                `zip_code`           int not null,
                `city`               char(32) not null,
                `state`              char(2) not null,
                `county`             char(32) not null,
                `median_age`         int not null,
                `total_rooms`        int not null,
                `total_bedrooms`     int not null,
                `population`         int not null,
                `households`         int not null,
                `median_income`      int not null,
                `median_house_value` int not null
               );
        """
        # Clear the housing table and create a new one, then commit the results to SQL
        cursor.execute(sqlDelete)
        myConnection.commit()
        cursor.execute(sqlCreate)
        myConnection.commit()

        # Create a function to insert the merged file data into the SQL database
        sqlInsert = """
            insert into
              housing (guid, zip_code, city, state, county, median_age, total_rooms, total_bedrooms, population, 
              households, median_income, median_house_value)
            values
              (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s);
            """

        # ===============
        # Execute insert function and commit results to SQL
        for i in dataIndices:
            cursor.execute(sqlInsert, (data.guid[i], data.zip_code[i], data.city[i], data.state[i], data.county[i],data.housing_median_age[i],
            data.total_rooms[i], data.total_bedrooms[i], data.population[i], data.households[i], data.median_income[i], data.median_house_value[i]))
        myConnection.commit()

        # Begin data validation using the SQL database
        print("Beginning validation\n")

        # Create a function to calculate the number of total bedrooms when the total number of rooms is greater than an input value
        sqlBedrooms = """
        SELECT sum(total_bedrooms) FROM housing WHERE total_rooms > %s
        """

        # Allow the user to input the number of total rooms and execute the Bedrooms function
        # Then print the number of total bedrooms
        rooms = int(input("Total Rooms: "))
        cursor.execute(sqlBedrooms, rooms)
        fetchList = cursor.fetchall()
        df = pd.DataFrame(fetchList)
        bedrooms = df.at[0, 'sum(total_bedrooms)']
        if bedrooms == None:
            bedrooms = 0
        print(f"For locations with more than {rooms} rooms, there are a total of {bedrooms} bedrooms.\n")

        # Create a function to pull the median_income for the input zip_code
        sqlIncome = """
                SELECT median_income FROM housing WHERE zip_code = %s;
                """

        # Allow the user to input the number of total rooms and execute the Income function
        # Then calculate the median value from the list and print the median income
        zipcode = int(input("ZIP Code: "))
        cursor.execute(sqlIncome, zipcode)
        incomeList = cursor.fetchall()
        df1 = pd.DataFrame(incomeList)
        if df1.empty:
            median = 0
        else:
            sortedFile = df1.sort_values(by='median_income')
            position = 0
            median = 0
            while True:
                if (len(sortedFile.median_income)+1)/2 != 0:
                    position = len(sortedFile.median_income)/2
                    median = sortedFile.median_income[position]
                    break
                elif (len(sortedFile.median_income)+1)/2 == 0:
                    position = (len(sortedFile.median_income)+1)/2
                    median = sortedFile.median_income[position]
                    break
        print(f"The median household income for ZIP code {zipcode} is {round(median):,}.\n")

# If there is an exception, show what that is
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()

# Close connection
finally:
    myConnection.close()
    print("Program exiting.")
