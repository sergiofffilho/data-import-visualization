import sys
import pandas as pd

# DATA IMPORT
# Read the CSV file
try:
    df = pd.read_csv('./assets/test_data_dummy.csv')
    print(f"Success read file.")
except FileNotFoundError:
    print("File not found.")
    sys.exit()

# Normalize data
df = df.map(lambda s:s.lower() if type(s) == str else s)
print("Data normalized.")

# Clear empty Ids
before = len(df)
df = df.dropna(subset=['Id'])
after = len(df)
print(f'Found {before - after} Ids empty - Removed')
print("------------------------------------------")

# Treat duplicated lines - creating new unique Ids
before = len(df)
df = df.drop_duplicates()
after = len(df)

df['Id'] = df['Id'] + df.groupby('Id').cumcount().astype(str)
print(f'Found {before - after} lines duplicated - Created uniqye ID to them')

# Convert LastReviewDate column to date/time type
df['LastReviewDate'] = pd.to_datetime(df['LastReviewDate'])

# Add "Continent" column
# 1st - Using a library (pycountry_convert)
import pycountry_convert as pc
def country_to_continent(country_name):
    try:
        country_alpha2 = pc.country_name_to_country_alpha2(country_name)
        country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
        return country_continent_name
    except:
        return 'Unknown country'
df['Continent'] = df['CountryCode'].str.title().apply(country_to_continent)

# 2nd - Using a native loop
continent_dict = {'south africa': 'Africa', 'italy': 'Europe','turkey': 'Asia' }  # Exemplo de dicionário
df['ContinentLoop'] = df['CountryCode'].map(continent_dict)

print(df)

''' 
Which method is more effective?

Both approaches have their advantages and can be more effective depending on the context.

- The first approach is more effective when you have a large dataset 
  and need a quick and easy-to-implement solution. The pycountry_convert library, for example,
  already has an internal mapping of countries to continents, which saves you the work of 
  creating and maintaining this mapping yourself. However, this approach depends on the accuracy 
  and up-to-dateness of the library. If the library does not recognize a country name or if the 
  data is outdated, you will get inaccurate results.

- The second one is more effective when you have full control over the data and need a custom solution. 
  With a loop, you can implement any logic you want to map countries to continents. This can be useful 
  if you have country names that are not recognized by the library or if you have a custom mapping of 
  countries to continents. However, this approach can be more time-consuming to implement and requires 
  more maintenance.
'''

# DATABASE
import sqlite3

# Create SQLITE database
conn = sqlite3.connect('db_data_analyst_test.db')

# Create cursor object
c = conn.cursor()

c.execute('PRAGMA foreign_keys = ON;')

# Create Country table
c.execute('''
    CREATE TABLE IF NOT EXISTS Country(
        Id TEXT PRIMARY KEY,
        Counterparty TEXT,
        CountryCode TEXT,
        CreditRegion TEXT,
        Continent TEXT,
        PrimaryOwner TEXT,
        CptyGroup TEXT,
        InsuranceCover TEXT,
        Division1 TEXT,
        LimitApplicationType TEXT,
        TotalLimit TEXT,
        Notes TEXT,
        Rating TEXT,
        CommercialSponsor TEXT
    )
''')

# Create Trade table
c.execute('''
    CREATE TABLE IF NOT EXISTS Trade(
        Id TEXT PRIMARY KEY,
        CountryTradeStatus TEXT,
        TradeStatus TEXT,
        CountryId TEXT,
        FOREIGN KEY(CountryId) REFERENCES Country(Id)
    )
''')

# Create Review table
c.execute('''
    CREATE TABLE IF NOT EXISTS Review(
        Id TEXT PRIMARY KEY,
        LastReviewDate TEXT,
        CountryId TEXT,
        FOREIGN KEY(CountryId) REFERENCES Country(Id)
    )
''')

# Create foreing key in the DataFrame to relate in the database tables
df['CountryId'] = df['Id']

# Define the tables and the corresponding columns in the DataFrame
tables = {
    "Country": ["Id", "Counterparty", "CountryCode", "CreditRegion", "Continent", "PrimaryOwner", "CptyGroup", "InsuranceCover", "Division1", "LimitApplicationType", "TotalLimit", "Notes", "Rating", "CommercialSponsor"],
    "Trade": ["Id", "CountryTradeStatus", "TradeStatus", "CountryId"],
    "Review": ["Id", "LastReviewDate", "CountryId"]
}

# Insert the data into the table in the SQLite database
for table, columns in tables.items():
    df_table = df[columns]
    df_table.to_sql(table, conn, if_exists='replace', index=False)

# VISUALIZATION
import matplotlib.pyplot as plt

# Read data from the database created
df = pd.read_sql_query('''
    SELECT Country.*, Review.LastReviewDate, Trade.CountryTradeStatus
        FROM Country
        JOIN Review ON Country.Id = Review.CountryId
        Join Trade ON Country.Id = Trade.CountryId
''', conn)

# Create a figure and a subplot set
fig, axs = plt.subplots(3, figsize=(15,10))

# 1st plot - Line chart
expired_counterparties = df[df['LastReviewDate'] < '2024-07-20']['CountryCode'].value_counts()
expired_counterparties.plot(kind='line', ax=axs[0])
axs[0].set_title('Number of Counterparties having its review date expired by Country')
axs[0].set_xlabel('')

# 2nd plot - Bar chart
active_counterparties = df[df['CountryTradeStatus'] == 'active']['Continent'].value_counts()
active_counterparties.plot(kind='bar', ax=axs[1])
axs[1].set_title('Number of Active Counterparties per Continent')
axs[1].set_xlabel('')

# Add subtitle values on the chart
for i in axs[1].patches:
    axs[1].text(i.get_x() + i.get_width() / 2, i.get_height() + 0.5, str(i.get_height()), fontsize=10, ha='center')

# 3rd plot - Pie chart
grouped_ratings = df['Rating'].value_counts()
grouped_ratings.plot(kind='pie', ax=axs[2], autopct='%1.1f%%')
axs[2].set_title('Grouped Ratings number')
axs[2].set_ylabel('')

# Ajust margin between subplots
plt.subplots_adjust(hspace=0.6)

# Show the final window
plt.show()