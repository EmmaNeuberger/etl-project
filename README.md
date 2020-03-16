## In this ETL Project a messy sales data set and item reference sheet will be used to extract transform and load data into a database management system. 

### Python, SQL, and SQL Alchemy tools are used to complete this project.

### Three cleaned tables will be loaded into PostgreSQL 


```python
import pandas as pd
```

## 1) Clean messy sales data set


```python
# Path data sets
sales_path = "messysalesdata.csv"

# Read csv in original format
messy_sales = pd.read_csv(sales_path)
messy_sales.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Sell-to Customer No_</th>
      <th>yr</th>
      <th>mo</th>
      <th>FiscalYear</th>
      <th>FiscalQuarter</th>
      <th>No_</th>
      <th>Posting Group</th>
      <th>units</th>
      <th>sales</th>
      <th>CustomerType</th>
      <th>Description</th>
      <th>Your Item Type Field!</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>2014</td>
      <td>Q4</td>
      <td>800501-0208</td>
      <td>FG SEED KI</td>
      <td>300</td>
      <td>$2,100.00</td>
      <td>Retail</td>
      <td>SK, 7-Pod, Cherry Tomato</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>2014</td>
      <td>Q4</td>
      <td>800528-0208</td>
      <td>FG SEED KI</td>
      <td>100</td>
      <td>$700.00</td>
      <td>Retail</td>
      <td>SK, 7 Pod, Grow Anything</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>2014</td>
      <td>Q4</td>
      <td>800544-0208</td>
      <td>FG SEED KI</td>
      <td>300</td>
      <td>$2,100.00</td>
      <td>Retail</td>
      <td>SK, 7- Pod, Chili Pepper</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>2014</td>
      <td>Q4</td>
      <td>970133-0100</td>
      <td>ACCESSORY</td>
      <td>30</td>
      <td>$465.00</td>
      <td>Retail</td>
      <td>Acc, AeroVoir with Stand</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>5</td>
      <td>2015</td>
      <td>Q1</td>
      <td>800500-0208</td>
      <td>FG SEED KI</td>
      <td>500</td>
      <td>$3,500.00</td>
      <td>Retail</td>
      <td>SK, 7- Pod, Gourmet Herb</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Begin cleaning data set
# Drop Your Item Type Field! Column which contains no data and FiscalYear which is redundant
clean_sales = messy_sales.drop(columns=["Your Item Type Field!", "FiscalYear"])
```


```python
# Clean column names - uniform formatting and clear titles
clean_sales.columns = clean_sales.columns.str.strip().str.lower()\
    .str.replace(' ', '').str.replace('(', '').str.replace(')', '')\
    .str.replace('-', '').str.replace('_', '')

clean_sales.rename(columns = {'no':'itemno', 'selltocustomerno':'customerno'}, inplace = True) 

print(clean_sales.columns)
```

    Index(['customerno', 'yr', 'mo', 'fiscalquarter', 'itemno', 'postinggroup',
           'units', 'sales', 'customertype', 'description'],
          dtype='object')
    


```python
# Data type conversions
clean_sales.dtypes

# Clean units and sales to remove objects in str that prevent conversion to numeric
clean_sales["units"] = clean_sales['units'].str.strip().str.replace(' ', '')\
    .str.replace(',', '').str.replace('.', '').str.replace('$', "")\
    .str.replace('-', '').str.replace('(', '').str.replace(')', '')

clean_sales["sales"] = clean_sales['sales'].str.strip().str.replace(' ', '')\
    .str.replace(',', '').str.replace('.', '').str.replace('$', "")\
    .str.replace('-', '').str.replace('(', '').str.replace(')', '')

# Change to correct data types - possible now without symbols in string
clean_sales["units"] = pd.to_numeric(clean_sales["units"])
clean_sales["sales"] =pd.to_numeric(clean_sales["sales"])

print(clean_sales.dtypes)
```

    customerno        object
    yr                 int64
    mo                 int64
    fiscalquarter     object
    itemno            object
    postinggroup      object
    units            float64
    sales            float64
    customertype      object
    description       object
    dtype: object
    


```python
# selltocustomerno column cleaning exploration
print(clean_sales["customerno"].unique())
```

    ['3XL001' 'ACE001' 'AGOPS' 'AGSALES' 'AM3001' 'AMA001' 'AMA002' 'AMA003'
     'AMA004' 'AMA005' 'AMA006' 'AMA007' 'AMA008' 'AMA009' 'AMA010' 'BED001'
     'BED003' 'BED004' 'BED005' 'CAN002' 'CIN001' 'COS001' 'CHE004' 'COS003'
     'COS005' 'CRA001' 'DIR105' 'COS002' 'COS004' 'DIR107' 'BJS001' 'DIR108'
     'DIR109' 'DIR111' 'FRY001' 'GMA001' 'GRO004' 'HAW002' 'HAY001' 'HOM011'
     'HOM014' 'HAM001' 'HSN001' 'HUD001' 'HOM015' 'HYD010' 'KOH001' 'KOH002'
     'LOW001' 'LOW002' 'LNL001' 'MAC001' 'MAC002' 'MEH001' 'MEH002' 'MIL003'
     'PRO004' 'QVC001' 'QVC003' 'MCG001' 'MEI001' 'SAM001' 'SAM002' 'SCO003'
     'SHA002' 'SIR001' 'SOC001' 'SPR003' 'STE003' 'SCO005' 'SCO006' 'SUR001'
     'SUR002' 'TAR001' 'TAR002' 'TAS001' 'TRA002' 'TRU001' 'WAL002' 'WAL005'
     'WAL006' 'WAL007' 'WAY002' 'WIL002' 'WOO002' 'WOO003' 'ZUL001']
    


```python
# fiscalquarter column cleaning exploration
print(clean_sales["fiscalquarter"].unique())
```

    ['Q4' 'Q1' 'Q2' 'Q3']
    


```python
# no. cleaning
print(clean_sales["itemno"].unique())
```

    ['800501-0208' '800528-0208' '800544-0208' ... '903121-1100' '900819-1100'
     '900822-1100']
    


```python
# postinggroup cleaning
clean_sales["postinggroup"] = clean_sales['postinggroup'].str.strip().str.replace(' ', '').str.lower()

print(clean_sales["postinggroup"].unique())

# Replace ag3garden entry with fggardens - ag3garden is a model and belongs under fggardens designation
clean_sales["postinggroup"] = clean_sales["postinggroup"].str.replace('ag3garden', 'fggardens')
print(clean_sales["postinggroup"].unique())
```

    ['fgseedki' 'accessory' 'fggardens' 'raw' 'displays' 'seedpod' 'component'
     'ag3garden']
    ['fgseedki' 'accessory' 'fggardens' 'raw' 'displays' 'seedpod' 'component']
    


```python
# customertype cleaning
clean_sales["customertype"] = clean_sales["customertype"].str.lower()
print(clean_sales["customertype"].unique())

```

    ['retail' 'dr']
    


```python
# Begin to clean a very messy description column
clean_sales["description"] = clean_sales['description'].str.strip()\
    .str.replace('.', '').str.replace('$', '')\
    .str.replace('(', '').str.replace(')', '').str.lower()\
    .str.replace('-', '').str.replace('pod', 'p')\
    .str.replace('sk', '').str.replace('acc', '').str.replace('rp', '')\
    .str.replace('pack', 'pk').str.replace('w/', '').str.replace('us', '')\
    .str.replace('eu', '').str.replace('china', '').str.replace('dlx', '')\
    .str.replace('seedstartingsystem', 'sss').str.replace('seedstartingsys', 'sss')\
    .str.replace('aggrowbowlv2growmedia', 'growbowlandgrowmedia').str.replace('ag', '')\
    .str.replace('display', '').str.replace('seedp', '').str.replace('std', '')\
    .str.replace('pop', '').str.replace('aerogdn', '').str.replace('aerogarden', '')\
    .str.replace('&', '').str.replace('/', '').str.replace("''", "").str.replace('pdq', '')\
    .str.replace('versal', '').str.replace('bat', '').str.replace('deluxe', '').str.replace('wb', '')\
    .str.replace('gherb', 'gh').str.replace('gourmetherb', 'gh').str.replace(' ','' )
# Observe more description cleaning needs
clean_sales.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customerno</th>
      <th>yr</th>
      <th>mo</th>
      <th>fiscalquarter</th>
      <th>itemno</th>
      <th>postinggroup</th>
      <th>units</th>
      <th>sales</th>
      <th>customertype</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800501-0208</td>
      <td>fgseedki</td>
      <td>300.0</td>
      <td>210000.0</td>
      <td>retail</td>
      <td>,7p,cherrytomato</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800528-0208</td>
      <td>fgseedki</td>
      <td>100.0</td>
      <td>70000.0</td>
      <td>retail</td>
      <td>,7p,growanything</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800544-0208</td>
      <td>fgseedki</td>
      <td>300.0</td>
      <td>210000.0</td>
      <td>retail</td>
      <td>,7p,chilipepper</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>970133-0100</td>
      <td>accessory</td>
      <td>30.0</td>
      <td>46500.0</td>
      <td>retail</td>
      <td>,aerovoirwithstand</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>5</td>
      <td>Q1</td>
      <td>800500-0208</td>
      <td>fgseedki</td>
      <td>500.0</td>
      <td>350000.0</td>
      <td>retail</td>
      <td>,7p,gourmetherb</td>
    </tr>
  </tbody>
</table>
</div>




```python
# More order dependent cleaning - replace all commas with spaces, then strip() to remove spaces from bookends of entry
clean_sales["description"] = clean_sales["description"].str.replace(',', ' ').str.strip()

# Replace spaces inside string to be able to separate cleaner values within description column
clean_sales["description"] = clean_sales["description"].str.replace(' ', ',')
clean_sales.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customerno</th>
      <th>yr</th>
      <th>mo</th>
      <th>fiscalquarter</th>
      <th>itemno</th>
      <th>postinggroup</th>
      <th>units</th>
      <th>sales</th>
      <th>customertype</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800501-0208</td>
      <td>fgseedki</td>
      <td>300.0</td>
      <td>210000.0</td>
      <td>retail</td>
      <td>7p,cherrytomato</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800528-0208</td>
      <td>fgseedki</td>
      <td>100.0</td>
      <td>70000.0</td>
      <td>retail</td>
      <td>7p,growanything</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>800544-0208</td>
      <td>fgseedki</td>
      <td>300.0</td>
      <td>210000.0</td>
      <td>retail</td>
      <td>7p,chilipepper</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>970133-0100</td>
      <td>accessory</td>
      <td>30.0</td>
      <td>46500.0</td>
      <td>retail</td>
      <td>aerovoirwithstand</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3XL001</td>
      <td>2014</td>
      <td>5</td>
      <td>Q1</td>
      <td>800500-0208</td>
      <td>fgseedki</td>
      <td>500.0</td>
      <td>350000.0</td>
      <td>retail</td>
      <td>7p,gourmetherb</td>
    </tr>
  </tbody>
</table>
</div>




```python
separated_sales = clean_sales["description"].str.split(",", expand=True)
separated_df = pd.DataFrame(separated_sales)
separated_df.head()

separated_df.to_csv("separated_description.csv")
```

## 2) Clean itemno reference data


```python
item_path = "itemlookup.csv"
messy_item = pd.read_csv(item_path)
messy_item.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ItemNumber</th>
      <th>Posting Group</th>
      <th>Description</th>
      <th>Add Your Item Type Field Here!</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0020-00Z</td>
      <td>FG SEED KI</td>
      <td>SK, 14 UnivPods, Salad Lover's</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0021-00Z</td>
      <td>FG SEED KI</td>
      <td>SK, 14P, Herb Lover's Seed Kit</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>100235-0000</td>
      <td>RAW</td>
      <td>Flat Plant Spacer (China)</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>100824-0000</td>
      <td>RAW</td>
      <td>SuperGrow Nutrients (3oz.)</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>100828-0000</td>
      <td>ACCESSORY</td>
      <td>Aerovoir Stand</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Drop unneccessary columns
clean_item = messy_item.drop(columns=["Add Your Item Type Field Here!"])

# Clean column names - uniform formatting and clear titles
clean_item.columns = clean_item.columns.str.strip().str.lower()\
    .str.replace(' ', '').str.replace('(', '').str.replace(')', '')\
    .str.replace('-', '').str.replace('_', '')

clean_item.rename(columns = {'itemnumber':'itemno'}, inplace = True) 

# Make sure clean_sales columns match clean_item keys
print(clean_item.columns)
print(clean_sales.columns)
```

    Index(['itemno', 'postinggroup', 'description'], dtype='object')
    Index(['customerno', 'yr', 'mo', 'fiscalquarter', 'itemno', 'postinggroup',
           'units', 'sales', 'customertype', 'description'],
          dtype='object')
    


```python
# check that dtypes are correct
clean_item.dtypes
```




    itemno          object
    postinggroup    object
    description     object
    dtype: object




```python
# itemno cleaning exploration
print(clean_item['itemno'].unique())

# strip just incase
clean_item['itemno'] = clean_item['itemno'].str.strip()

```

    ['0020-00Z' '0021-00Z' '100235-0000' ... '976926-0100' '976927-0000'
     '976950-0100']
    


```python
# postinggroup cleaning
clean_item['postinggroup'].unique()
clean_item["postinggroup"] = clean_item['postinggroup'].str.strip().str.replace(' ', '').str.lower()

# Make sure clean_sales columns match clean_item postinggroup keys
print(clean_item["postinggroup"].unique())
print(clean_sales["postinggroup"].unique())

# Item reference data has more postinggroup options than sales data - item reference will/ 
# contain posting group data for clarity and most likely accuracy - real world situation would require/
# a conversation with database administrator
```

    ['fgseedki' 'raw' 'accessory' 'fggardens' 'seedpod' 'displays' 'seedkits'
     'aerogarden' 'component']
    ['fgseedki' 'accessory' 'fggardens' 'raw' 'displays' 'seedpod' 'component']
    


```python
# description cleaning clean_item with same methods as clean_sales
clean_item["description"] = clean_item['description'].str.strip()\
    .str.replace('.', '').str.replace('$', '')\
    .str.replace('(', '').str.replace(')', '').str.lower()\
    .str.replace('-', '').str.replace('pod', 'p')\
    .str.replace('sk', '').str.replace('acc', '').str.replace('rp', '')\
    .str.replace('pack', 'pk').str.replace('w/', '').str.replace('us', '')\
    .str.replace('eu', '').str.replace('china', '').str.replace('dlx', '')\
    .str.replace('seedstartingsystem', 'sss').str.replace('seedstartingsys', 'sss')\
    .str.replace('aggrowbowlv2growmedia', 'growbowlandgrowmedia').str.replace('ag', '')\
    .str.replace('display', '').str.replace('seedp', '').str.replace('std', '')\
    .str.replace('pop', '').str.replace('aerogdn', '').str.replace('aerogarden', '')\
    .str.replace('&', '').str.replace('/', '').str.replace("''", "").str.replace('pdq', '')\
    .str.replace('versal', '').str.replace('bat', '').str.replace('deluxe', '').str.replace('wb', '')\
    .str.replace('gherb', 'gh').str.replace('gourmetherb', 'gh').str.replace(' ', '')
# Observe more description cleaning needs
clean_item.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>itemno</th>
      <th>postinggroup</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0020-00Z</td>
      <td>fgseedki</td>
      <td>,14univps,saladlover's</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0021-00Z</td>
      <td>fgseedki</td>
      <td>,14p,herblover'sseedkit</td>
    </tr>
    <tr>
      <th>2</th>
      <td>100235-0000</td>
      <td>raw</td>
      <td>flatplantspacer</td>
    </tr>
    <tr>
      <th>3</th>
      <td>100824-0000</td>
      <td>raw</td>
      <td>supergrownutrients3oz</td>
    </tr>
    <tr>
      <th>4</th>
      <td>100828-0000</td>
      <td>accessory</td>
      <td>aerovoirstand</td>
    </tr>
  </tbody>
</table>
</div>




```python
# More order dependent cleaning - replace all commas with spaces, then strip() to remove spaces from bookends of entry
clean_item["description"] = clean_item["description"].str.replace(',', ' ').str.strip().str.replace('  ', ' ')

clean_item.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>itemno</th>
      <th>postinggroup</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0020-00Z</td>
      <td>fgseedki</td>
      <td>14univps saladlover's</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0021-00Z</td>
      <td>fgseedki</td>
      <td>14p herblover'sseedkit</td>
    </tr>
    <tr>
      <th>2</th>
      <td>100235-0000</td>
      <td>raw</td>
      <td>flatplantspacer</td>
    </tr>
    <tr>
      <th>3</th>
      <td>100824-0000</td>
      <td>raw</td>
      <td>supergrownutrients3oz</td>
    </tr>
    <tr>
      <th>4</th>
      <td>100828-0000</td>
      <td>accessory</td>
      <td>aerovoirstand</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Separate description into columns to make data more useful
separated_item = clean_item["description"].str.split(" ", expand=True)
separated_item.head()

# Find which columns have uneccessary data
print(separated_item[0].unique())
print("................................................................")
print("................................................................")

print(separated_item[1].unique())
print("................................................................")
print("................................................................")

print(separated_item[2].unique())
print("................................................................")
print("................................................................")

print(separated_item[3].unique())
print("................................................................")
print("................................................................")

print(separated_item[4].unique())
print("................................................................")
print("................................................................")

separated_item.drop(columns=[4])

```

    ['14univps' '14p' 'flatplantspacer' 'supergrownutrients3oz'
     'aerovoirstand' '7aerator100610' 'seedp' '7p' '7' '7ghcherrytomato'
     '7led' '3' '3sl' 'sproutplblk' 'ctc' "macy's" 'bbb' "kohl's" 'costco'
     "macy's2019" 'kohls' 'bedhbeyond' 'acehardware' 'ctc12pallet'
     'ctc14pallet' 'herbie' 'harvestelite' 'harvestplgrey'
     '3slwalmartdsplyprod' '6' 'plant' 'extra' 'extraled' '3p'
     '7pmegacherrytomato' 'seedstartingsystem' 'seedstartingsys'
     'seedstartingsystemsprout' 'seedstartingsystemharvest'
     'seedstartingsystemharvst' 'seedstartingsystemharvstfr'
     'seedstartingsystembounty' '2x3p' '2x' '3pviolayellowredwing'
     '3pvibrantviolas_2pk' '3pzinniamix' 'uni' 'growanything25pk'
     'growanything50pk' '6p' 'seedstartingsystem6' 'gardenstartertray'
     'seedstartingsystem6plstc' '7pgourmetherbseedkit' '12p' 'gf'
     '6pgrowanything' '6phrlmsaladgreens' '9p' '9pmightymini'
     '9phrlmsaladgreens' 'ultraledbundle' 'farm' '3penguin' '7silver' '3psl'
     '3pslwhite' 'sprout' 'sprt' 'sproutred' 'sproutled' 'farmpl' 'farmxl'
     '6ss6mdlblk' '6led' '6ledwithgh' '6ledwithghct' '6ledwithghsss' 'harvest'
     'harvesttouch' 'hrvsttouch' 'harvestelitecopper' 'harvestelite2015'
     'aerogdharvestelite' 'harvestelitetouch' 'harvestpremium' 'harvest200'
     'harvestpl' 'harvest2016' 'harvestwifi' 'harvestelitewifi' 'harvest2018'
     'harvestelite2018' 'harvest2019' 'harvest360' 'harvestelite360'
     'gfharvestslim' 'harvestslim' 'harvesteliteslim' 'harvestelite2019'
     'ultra' 'ultraled' 'bounty' 'bountywithwifi' 'bountyelite'
     'bountyeliteplatinum' 'bountyelitewifi' 'bountyeliteredwifi'
     'bountybasic2019' 'bounty2019' 'bountyelite2019' 'bountyeliteartisan'
     'herbnserve' 'pumpfilter' 'growbulbs' 'bowl' 'base' 'post' 'hood' 'esn'
     'gstrefillkit' 'spacerplugs' 'plantspacers' 'bethebee' 'bulbadapter'
     'aerovoirwithstand' 'mezzalunaherbchopper' 'mezzalunawith'
     'farmstackingkit' 'bounty2019trellissystem' 'bamboowallstand'
     'bamboodestandwithdrawer' 'herbkeeper' 'herbnsave' 'trellissystem'
     'coils' 'softtiesforplants' 'easyupplantsupports' 'wateringcan'
     'herbshears' 'cuttingboard' 'bulkbaetsuni' 'bulkseedplabels' 'bulkdomes'
     'powergrowlightbooster' 'supergrownutrientsl' 'easyfeeddispensers3pk'
     'activeeyebugloupe30x' 'aquapro' 'pheptester' 'ghphdown' 'ghphup' 'peat'
     'easystartgrowlightsystem' 'oxygenboosterkit' 'growbowl' 'grobwlstrarrys'
     'growmediaforewith' 'groowlandgrowmedia' 'spout' 'wateringcanwithspout'
     'growbowlv2' 'grobwlv2strarrys' 'growbowlv2growmedia' 'siphonpump' 'book'
     'groable18pkassorted' 'groablebasilseedp' 'groblecherrytomatoseedp'
     'groablecilantroseedp' 'groableglobetomatoseedp' 'groablecucumberseedp'
     'groablesweetpepperseedp' 'maxgrowlight' '12wattledgrowlight'
     '12wattledgrowlight2pk' '12wattledgrowlight4pk' '20wattledgrowlight'
     '20wattledgrowlight2pk' '20wattledgrowlight4pk'
     '45wattledgrowpanel+stand' 'float' 'airstone' 'pump' 'farmplmetalframe'
     'farmmetalframe' 'farmpltop' 'farmtop' 'farmfarmplbowl' 'farmfarmplled'
     'farmfarmplctrlbox' 'farmxlmetalframe' 'cord' 'harvestblkbasehood'
     'harvestwhtbasehood' 'harvestelitetchrdbasehood' 'harvest2016blkbasehood'
     'harvestwifiblkbowl' 'harvestwhtbowl' 'harvestelitetouchbowl'
     'harvestelitetouchredbowl' 'harvestredbowl' 'harvesteliteplatinumbowl'
     'harvestplgreybasehood' 'harvestplredbasehood' 'harvestelitepltbasehood'
     'harvestgrowdeckwhite' 'harvestplgreybowl' 'harvestplredbowl'
     'harvestpltpremiumbasehood' 'harvestwifiblackbasehood' 'basehood'
     'harvestblkpremiumbowl' 'harvestpltpremiumbowl' 'harvestredpremiumbowl'
     'portcover' 'harvesttouchblkbasehood' 'harvesttoucheggbowl'
     'harvesttouchblkbowl' 'harvesttouchwhtbowl' 'harvestelitewifiredbowl'
     'cflhood' 'growdeck' 'post66ledmdl2014' 'ap30wledhood' 'ap30wledhoodv3'
     'trellis' 'ultraupgradekit' 'bountyhood' 'bountyelitesshood'
     'bountyeliteredhood' 'bountyeliteplthood' 'bountybountyelitegrowdeck'
     'bountyelitessbowl' 'bountyeliteredbowl' 'bountyelitepltbowl'
     'bountybountyelitebase' 'bountyeliteredbase' 'bountyelitepltbase'
     'bountywifibase' 'bountyelitewifibase' 'bountyeliteredwifibase'
     'bountyelitepltwifibase' 'harvest2018model']
    ................................................................
    ................................................................
    ["saladlover's" "herblover'sseedkit" None 'blk' '7p' 'tomato' 'holybasil'
     'basil' 'catnip' 'shungiku' 'chivesm' 'tarront' 'cilantrom' 'dillt'
     'tomatoredh' 'chilipepper' 'pepper' 'parsley' 'marjorams' 'mint'
     'oreganos' 'sem' 'vinca' 'phlox' 'saladgreens' 'minipetunia' 'romaine'
     'lavenders' 'gazania' 'zinnia' 'lemonbalm' 'thaibasil' 'stock' 'kale'
     'viola' 'ghostpepper' 'anaheimpepper' 'poblanopepper' 'cayennepepper'
     'thymes' 'babygreens' 'arugula' 'tatsoi' 'mtard' 'cress'
     'blackseededsimpson' "rouged'hiver" 'marvelof4seasons' 'cascpetunia'
     'minicherrytomato' 'rosemary' 'collardgreens' 'silver' 'slv' 'wht'
     'black' 'bundle' 'loaded' 'original2019' '2019' 'activitybook' 'bbb2015'
     'bbb' 'livestraerry' 'ctomkit' 'ctomkitsalad' 'ctomkitflower'
     'autumncolors' 'loveblooms' 'bounty2019models' 'uni' 'harvest' '7'
     'exbtyul' 'farm' 'hrvt2018' 'hrvst2018' 'hrvtslim' 'hrvt360'
     'cascadingpetunia' 'minijalapeno' 'growanything' 'growanythingx2'
     'pestobasil' '3p' 'gourmetherb' 'saladgreensmix' 'mightyminicherrytomato'
     'mightyminicherrytom' 'lavenderkit' 'heirloomsaladgreens'
     'heirloomsaladgrns' 'pizzaherbs' 'polkadotmix' 'kalemix' 'springflowers'
     'springflowers_2pk' 'phloxinabox' 'phloxinabox_2pk' 'beetgreens'
     'beetgreens_2pk' 'rosemary_2pk' 'smoothiegreens' 'smoothiegrns_2pk'
     'freshtea' 'bokchoi' 'swisschard' 'southerngreens' 'cherrytomato'
     'tcanitalianherb' 'cole' 'goldentomato' 'mixedromaine' 'growanythingkit'
     'englishcotte' 'mountainmeadow' 'splashofcolor' 'bellpeppers'
     'internationalbasil' 'salsagarden' 'lavender' 'ss6' "flowerlover's"
     'freshtastyherb' 'freshtastysalad' 'freshtastytomato' 'thaipepper'
     'jalapenopepper' 'uplandcress' 'chinesecabbe' 'tradlmedicinal'
     'straerryliveplants' 'asianherbs' 'pizzaherb' 'celosia'
     'cocktailmocktail' 'incredibleedibles' 'violayellowrdwing' 'zinniamix'
     'scarboroughfair' 'pizzaparty' 'lettucefortwo' 'minticecream'
     'snapdrpuppetshow' 'grmthrbhrlmtomato' 'cascpetunia_obsolet'
     'cascadingpetuniax2' '6p' 'gourmetherbx2' 'hrlmcherrytomato'
     'cherrytomatox2' 'italianherb' 'ca' 'mightymini' 'jalepenopepper'
     'tradlmedicinl' 'hrlmsaladgreens' '2pk' 'heirloomsaladgrnsx2'
     'incredibleed' 'smoothiegrns' 'scarboroughfa' 'ctomherb'
     'megacherrytomato' 'intlbasil' 'pizzaherbkit' 'shishitopeppers'
     'sweetbananapeppers' 'fairytaleeggplant' 'fajitapepper' 'jumboveggies'
     'ctomvegetable' 'ctomsalad' 'ctomflower' 'hrlmtomato' 'gh+sss'
     'greatgreens' 'saladbar' 'pink' 'yellow' 'spottedcow' 'penguin' 'gherb'
     'bee' 'ladybug' 'blackno' 'no' 'white' 'blue' 'blu' 'red' 'kh+minict'
     'teal' 'grey' 'tasty' 'gh' 'eggplnt' 'egg' 'ssgh' 'plt' 'ss' 'rd' 'cp'
     'cpr' 'copper' 'gmg' 'eggplant' 'tel' 'eggplt' 'blkuk' 'whtuk' 'sge' 'se'
     'gry' 'coolgray' 'sscascpetunias' 'gww' 'cor' 'pwh' 'pbh' 'pco' '2'
     'blk2s' 'bonpck' 'gh+hct' 'wifi' 'pre' 'stb' 'ppl' 'pbg' 'awh' '10pk'
     'stainless' 'retail' '6spacers' '18pk' '25pk' 'trellissystem' '4cb' "25'"
     'oxo' 'green' 'wooden' 'bulk' 'roll' 'mstrchef' '50ways' '1pk' '6pk'
     '3pk' 'triplepk' '7ponly' '5pk' 'le' 'ri' 'left' 'right' 'ledpowercord'
     'ceramic' 'ultra' 'stl' 'silve' 'elite6+veggiepr' 'veggiepro' 'aluminum'
     '6led' '66led2014mdl' '66ledmdl2014' 'poweradapter']
    ................................................................
    ................................................................
    [None 'celosia' 'genov' 'bell' 'lemon' 'no' 'gh' 'gherb' 'wmcom' 'ca'
     'pizza' 'lettuce' 'icmint' 'snapdrg' 'herb' 'veg' 'salad' 'flower'
     'veggie' 'pestobasil' '2pk' 'pizzaherbs' 'kalemix' 'pkin' 'stand'
     'gourmetherb' 'cherrytomato' 'growanything' 'hrlmsaladgreens'
     'pizzaherbkit' 'uk' 'a' 'b' 'gherbs' '2s' 'bb' 'kh+minict' 'gh+minict'
     'sss' 'ghx2' 'ranch' 'southwestern' 'capresesalad' 'cocktailmocktail'
     'sssinstore' 'ghhct' 'sssbundle' 'mchguide' 'fr' 'hct' 'ss' '3uround'
     '7p' 'pro100' '4pk' 'blue' '50pk' 'spiral' 'blk' 'extra' '3' '3elite'
     'ss6' 'ss6ps' 'ss6elite' '7' '7mdl100610' '6' 'elite6' '3sl100303wht'
     'sprout100303blk' 'sprout100303wht' 'sproutpl' '3sl100303blk'
     'sprout100303red' 'sproutsproutpl' 'sproutled' '7mdl2014' 'blackhood'
     '6ss6mdl2014' 'ultraextra' 'ultraled30w' '7led' 'extraled30w' 'bounty'
     'ultra' 'extra555' 'extra2238' 'extra29999']
    ................................................................
    ................................................................
    [None 'ga' 'sss' 'ca' 'pkin' 'sg' 'ct' 'shears' 'bb' 'bounty' 'ss6ps'
     'extra']
    ................................................................
    ................................................................
    [None 'harves' 'bounty']
    ................................................................
    ................................................................
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>0</th>
      <th>1</th>
      <th>2</th>
      <th>3</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>14univps</td>
      <td>saladlover's</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>14p</td>
      <td>herblover'sseedkit</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>flatplantspacer</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>supergrownutrients3oz</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>aerovoirstand</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>7aerator100610</td>
      <td>blk</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>seedp</td>
      <td>7p</td>
      <td>celosia</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>seedp</td>
      <td>tomato</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>seedp</td>
      <td>holybasil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>seedp</td>
      <td>basil</td>
      <td>genov</td>
      <td>None</td>
    </tr>
    <tr>
      <th>10</th>
      <td>seedp</td>
      <td>basil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>11</th>
      <td>seedp</td>
      <td>basil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>12</th>
      <td>seedp</td>
      <td>basil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>13</th>
      <td>seedp</td>
      <td>basil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>14</th>
      <td>seedp</td>
      <td>basil</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>15</th>
      <td>seedp</td>
      <td>catnip</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>16</th>
      <td>seedp</td>
      <td>shungiku</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>17</th>
      <td>seedp</td>
      <td>chivesm</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>18</th>
      <td>seedp</td>
      <td>tarront</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>19</th>
      <td>seedp</td>
      <td>cilantrom</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>20</th>
      <td>seedp</td>
      <td>dillt</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>21</th>
      <td>seedp</td>
      <td>tomato</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>22</th>
      <td>seedp</td>
      <td>tomatoredh</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>23</th>
      <td>seedp</td>
      <td>chilipepper</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>24</th>
      <td>seedp</td>
      <td>pepper</td>
      <td>bell</td>
      <td>None</td>
    </tr>
    <tr>
      <th>25</th>
      <td>seedp</td>
      <td>pepper</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>26</th>
      <td>seedp</td>
      <td>pepper</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>27</th>
      <td>seedp</td>
      <td>parsley</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>28</th>
      <td>seedp</td>
      <td>parsley</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>29</th>
      <td>seedp</td>
      <td>marjorams</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1033</th>
      <td>hood</td>
      <td>black</td>
      <td>7led</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1034</th>
      <td>hood</td>
      <td>black</td>
      <td>extraled30w</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1035</th>
      <td>ap30wledhood</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1036</th>
      <td>ap30wledhoodv3</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1037</th>
      <td>trellis</td>
      <td>ultra</td>
      <td>extra</td>
      <td>bounty</td>
    </tr>
    <tr>
      <th>1038</th>
      <td>bowl</td>
      <td>blk</td>
      <td>ultraextra</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1039</th>
      <td>bowl</td>
      <td>blk</td>
      <td>bounty</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1040</th>
      <td>base</td>
      <td>black</td>
      <td>ultra</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1041</th>
      <td>base</td>
      <td>black</td>
      <td>extra555</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1042</th>
      <td>base</td>
      <td>black</td>
      <td>extra2238</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1043</th>
      <td>base</td>
      <td>black</td>
      <td>extra29999</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1044</th>
      <td>post</td>
      <td>black</td>
      <td>ultra</td>
      <td>extra</td>
    </tr>
    <tr>
      <th>1045</th>
      <td>ultraupgradekit</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1046</th>
      <td>bountyhood</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1047</th>
      <td>bountyelitesshood</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1048</th>
      <td>bountyeliteredhood</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1049</th>
      <td>bountyeliteplthood</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1050</th>
      <td>bountybountyelitegrowdeck</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1051</th>
      <td>bountyelitessbowl</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1052</th>
      <td>bountyeliteredbowl</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1053</th>
      <td>bountyelitepltbowl</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1054</th>
      <td>bountybountyelitebase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1055</th>
      <td>bountyeliteredbase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1056</th>
      <td>bountyelitepltbase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1057</th>
      <td>bountywifibase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1058</th>
      <td>bountyelitewifibase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1059</th>
      <td>bountyeliteredwifibase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1060</th>
      <td>bountyelitepltwifibase</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1061</th>
      <td>harvest2018model</td>
      <td>poweradapter</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1062</th>
      <td>hood</td>
      <td>white</td>
      <td>7mdl2014</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>1063 rows × 4 columns</p>
</div>




```python
# Add in separated descriptions to clean_item
clean_item["description1"] = separated_item[0]
clean_item["description2"] = separated_item[1]
clean_item["description3"] = separated_item[2]
clean_item["description4"] = separated_item[3]
```


```python
# export clean_item to csv
clean_item.to_csv("cleanitemlookup.csv", index=False)
```

## 3) Clean customerno reference data


```python
customer_path = "customerlookup.csv"
messy_customer = pd.read_csv(customer_path)
messy_customer.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Sell-to Customer No_</th>
      <th>CustomerType</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3XL001</td>
      <td>Retail</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ACE001</td>
      <td>Retail</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AGOPS</td>
      <td>Retail</td>
    </tr>
    <tr>
      <th>3</th>
      <td>AGSALES</td>
      <td>Retail</td>
    </tr>
    <tr>
      <th>4</th>
      <td>AM3001</td>
      <td>Retail</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Clean columns
clean_customer = messy_customer

clean_customer.columns = clean_customer.columns.str.strip().str.lower()\
    .str.replace(' ', '').str.replace('(', '').str.replace(')', '')\
    .str.replace('-', '').str.replace('_', '')

clean_customer.rename(columns = {'selltocustomerno':'customerno'}, inplace = True) 

# Keys match
print(clean_customer.columns)
print(clean_item.columns)
print(clean_sales.columns)
```

    Index(['customerno', 'customertype'], dtype='object')
    Index(['itemno', 'postinggroup', 'description', 'description1', 'description2',
           'description3', 'description4'],
          dtype='object')
    Index(['customerno', 'yr', 'mo', 'fiscalquarter', 'itemno', 'postinggroup',
           'units', 'sales', 'customertype', 'description'],
          dtype='object')
    


```python
# Clean customerno
print(clean_customer["customerno"].unique())

clean_customer["customerno"] = clean_customer["customerno"].str.strip()
```

    ['3XL001' 'ACE001' 'AGOPS' 'AGSALES' 'AM3001' 'AMA001' 'AMA002' 'AMA003'
     'AMA004' 'AMA005' 'AMA006' 'AMA007' 'AMA008' 'AMA009' 'AMA010' 'BED001'
     'BED003' 'BED004' 'BED005' 'BJS001' 'CAN002' 'CHE004' 'CIN001' 'COS001'
     'COS002' 'COS003' 'COS004' 'COS005' 'CRA001' 'DIR011' 'DIR105' 'DIR107'
     'DIR108' 'DIR109' 'DIR110' 'DIR111' 'FRY001' 'GLO002' 'GMA001' 'GRO004'
     'HAM001' 'HAW002' 'HAY001' 'HOM011' 'HOM014' 'HOM015' 'HSN001' 'HUD001'
     'HYD005' 'HYD010' 'HYD011' 'KOH001' 'KOH002' 'LIF002' 'LNL001' 'LOW001'
     'LOW002' 'MAC001' 'MAC002' 'MCG001' 'MEH001' 'MEH002' 'MEI001' 'MIL003'
     'OPE001' 'PRO004' 'QVC001' 'QVC003' 'SAM001' 'SAM002' 'SCO003' 'SCO005'
     'SCO006' 'SER002' 'SHA002' 'SIR001' 'SOC001' 'SPR003' 'STE003' 'SUR001'
     'SUR002' 'TAR001' 'TAR002' 'TAS001' 'TRA002' 'TRU001' 'WAL002' 'WAL005'
     'WAL006' 'WAL007' 'WAY002' 'WIL002' 'WOO002' 'WOO003' 'ZUL001']
    


```python
# Clean customertype
print(clean_customer["customertype"].unique())

clean_customer["customertype"] = clean_customer["customertype"].str.strip().str.lower()

print(clean_customer["customertype"].unique())
```

    ['Retail' 'DR']
    ['retail' 'dr']
    


```python
# export clean_customer to csv
clean_customer.to_csv("cleancustomerlookup.csv", index=False)
```

## 4) Create paired down sales data table for reference




```python
print(clean_sales.columns)
# remove 'postinggroup', 'description', and 'customertype' which can be accessed through customerreference and item reference
```

    Index(['customerno', 'yr', 'mo', 'fiscalquarter', 'itemno', 'postinggroup',
           'units', 'sales', 'customertype', 'description'],
          dtype='object')
    


```python
clean_sales = clean_sales[['itemno', 'customerno', 'yr', 'mo', 'fiscalquarter', 'units', 'sales' ]]
clean_sales.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>itemno</th>
      <th>customerno</th>
      <th>yr</th>
      <th>mo</th>
      <th>fiscalquarter</th>
      <th>units</th>
      <th>sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>800501-0208</td>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>300.0</td>
      <td>210000.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>800528-0208</td>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>100.0</td>
      <td>70000.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>800544-0208</td>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>300.0</td>
      <td>210000.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>970133-0100</td>
      <td>3XL001</td>
      <td>2014</td>
      <td>1</td>
      <td>Q4</td>
      <td>30.0</td>
      <td>46500.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>800500-0208</td>
      <td>3XL001</td>
      <td>2014</td>
      <td>5</td>
      <td>Q1</td>
      <td>500.0</td>
      <td>350000.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Export clean_sales dataframe to csv
clean_sales.to_csv('cleansalesdata.csv', index=False) 
```


```python

```
