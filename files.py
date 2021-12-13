# Brian D. Christman
# Purpose: To read in and designate each of the housing files from the directory
# Enter the file paths for each file below

# Import necessary packages
import pandas as pd

# Designate each of the housing files
housingFile = pd.read_csv('C:\\Users\\bchri\\OneDrive\\Documents\\HousingProject-master\\files\\housing-info.csv')
incomeFile = pd.read_csv('C:\\Users\\bchri\\OneDrive\\Documents\\HousingProject-master\\files\\income-info.csv')
zipFile = pd.read_csv('C:\\Users\\bchri\\OneDrive\\Documents\\HousingProject-master\\files\\zip-city-county-state.csv')
