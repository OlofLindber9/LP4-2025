import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, format_string
from pyspark.sql.types import IntegerType
import pandas as pd
import sys

@udf(returnType=IntegerType())
def decade(dt):
    """
    Computes the decade for a given date.
    Parameters:
    - dt, datetime : the Gregorian date for which to compute the decade

    Return value: an integer denoting the decade (e.g. 1910s = 1910)
    """
    y = dt.year
    decade = y // 10 * 10
    return decade

@udf(returnType=IntegerType())
def jdn(dt):
    """
    Computes the Julian date number for a given date.
    Parameters:
    - dt, datetime : the Gregorian date for which to compute the number

    Return value: an integer denoting the number of days since January 1, 
    4714 BC in the proleptic Julian calendar.
    """
    y = dt.year
    m = dt.month
    d = dt.day
    if m < 3:
        y -= 1
        m += 12
    a = y//100
    b = a//4
    c = 2-a+b
    e = int(365.25*(y+4716))
    f = int(30.6001*(m+1))
    jd = c+d+e+f-1524
    return jd

    
# you probably want to use a function with this signature for computing the
# simple linear regression with least squares using applyInPandas()
# key is the group key, df is a Pandas dataframe
# should return a Pandas dataframe
def lsq(df):
    x = df['JDN']
    y = df['TAVG']

    x_bar = x.mean()
    y_bar = y.mean()

    dx = x - x_bar
    dy = y - y_bar
    
    numerator = (dx * dy).sum()
    denominator = (dx ** 2).sum()

    beta = numerator / denominator if denominator != 0 else float('nan')
    alpha = y_bar - beta * x_bar if pd.notnull(beta) else float('nan')

    return pd.DataFrame([{
        'STATION': df['STATION'].iloc[0],
        'ALPHA': alpha,
        'BETA': beta
    }])   

def decade_avg(df):
    """
    Computes the average temperature for a decade.
    Parameters:
    - df, pandas dataframe : the dataframe containing the data

    Return value: a pandas dataframe with the average temperature for the decade
    """
    temp = df['TAVG']
    # Compute the average temperature for each decade
    avg_temp = temp.mean()
    return pd.DataFrame([{
        'STATION': df['STATION'].iloc[0],
        'DECADE': df['DECADE'].iloc[0],
        'TAVG': avg_temp
    }])

def compute_decade_diff(df):
    """
    Computes the difference between the average temperature of the 2010s and the 1910s.
    Parameters:
    - df, pandas dataframe : the dataframe containing the data

    Return value: a pandas dataframe with the difference between the average temperature of the 2010s and the 1910s
    """
    # Filter for 2010s and 1910s
    t2010 = df[df['DECADE'] == 2010]['TAVG']
    t1910 = df[df['DECADE'] == 1910]['TAVG']

    if t2010.empty or t1910.empty:
        diff = float('nan')
    else:
        diff = t2010.iloc[0] - t1910.iloc[0]
        
    return pd.DataFrame([{
        'STATION': df['STATION'].iloc[0],
        'DIFF': diff
    }])

if __name__ == '__main__':
    # do not change the interface
    parser = argparse.ArgumentParser(description = \
                                    'Compute climate data.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    # this bit is important: by default, Spark only allocates 1 GiB of memory 
    # which will likely cause an out of memory exception with the full data
    spark = SparkSession.builder \
            .master(f'local[{args.num_workers}]') \
            .config("spark.driver.memory", "16g") \
            .getOrCreate()
    
    # read the CSV file into a pyspark.sql dataframe and compute the things you need
    df = spark.read.csv(args.filename, header=True, inferSchema=True)

    # Compute the Julian date number for each date
    df = df.withColumn('JDN', jdn(col('DATE')))
    # Compute T_avg
    df = df.withColumn('TAVG', (col('TMAX') + col('TMIN')) / 2.0)
    # For each station, performs fits a simple linear regression model using 
    # least squares (T_avg as function of JDN)
    pd_lsq = df.groupBy('STATION').applyInPandas(lsq, schema='STATION STRING, ALPHA DOUBLE, BETA DOUBLE')

    # Add the station name to the dataframe
    station_names = df.select('STATION', 'NAME').dropDuplicates(['STATION'])
    pd_lsq_named = pd_lsq.join(station_names, on='STATION', how='left')
    # Print the station code, station name, and the slope of the top 5 stations with the greatest slope
    pd_lsq_sorted = pd_lsq_named.orderBy(col('BETA').desc()) 
    # .select('STATION', 'NAME', col('BETA').alias('SLOPE')) \
    #     .limit(5)
    
    # # top 5 slopes are printed here
    # print('Top 5 coefficients:')
    # for row in pd_lsq_sorted.limit(5).collect():
    #     STATIONCODE = row['STATION']
    #     STATIONNAME = row['NAME']
    #     BETA = row['BETA']
    #     print(f'{STATIONCODE} at {STATIONNAME} BETA={BETA:0.3e} °F/d')

    # # replace None with an appropriate expression
    # print('Fraction of positive coefficients:')
    # fraction_positive = pd_lsq_sorted.filter(col('BETA') > 0).count() / pd_lsq_sorted.count()
    # print(fraction_positive)

    # Prints the five-number summary of the slopes,
    # i.e. minimum, first quartile, median, third quartile, and maximum
    # pd_lsq = pd_lsq.withColumn('BETA', pd_lsq['BETA'].cast('double'))
    # five_num_sum = pd_lsq \
    #     .approxQuantile('BETA', [0, 0.25, 0.5, 0.75, 1], 0.0)
    # five_num_sum.show()

    # Five-number summary of slopes, replace with appropriate expressions
    # print('Five-number summary of BETA values:')
    # beta_min, beta_q1, beta_median, beta_q3, beta_max = 5*[0.0]
    # print(f'beta_min {beta_min:0.3e}')
    # print(f'beta_q1 {beta_q1:0.3e}')
    # print(f'beta_median {beta_median:0.3e}')
    # print(f'beta_q3 {beta_q3:0.3e}')
    # print(f'beta_max {beta_max:0.3e}')

    # # Here you will need to implement computing the decadewise differences 
    # # between the average temperatures of 1910s and 2010s
    # # Compute the average temperature for each decade
    df_celsius = df.withColumn('TAVG', (col('TAVG') - 32) * 5.0 / 9)
    df_decade = df_celsius.withColumn('DECADE', decade(col('DATE')))
    df_decade_temp = df_decade.groupBy('STATION', 'DECADE') \
        .applyInPandas(decade_avg, schema='STATION STRING, DECADE INT, TAVG DOUBLE')
    # Compute the difference between the average temperature of the 2010s and the 1910s
    df_decade_diff = df_decade_temp.groupBy('STATION') \
        .applyInPandas(compute_decade_diff, schema='STATION STRING, DIFF DOUBLE')
    
    # Add the station name to the dataframe
    station_names = df.select('STATION', 'NAME').dropDuplicates(['STATION'])
    df_decade_diff = df_decade_diff.join(station_names, on='STATION', how='left')

    df_decade_diff_sorted = df_decade_diff.orderBy(col('DIFF').desc())

    # Compute the difference between the average temperature of the 2010s and the 1910s
    # df_decade_2010s = df_decade.filter(col('DECADE') == 2010)
    # df_decade_1910s = df_decade.filter(col('DECADE') == 1910)

    # There should probably be an if statement to check if any such values were 
    # computed (no suitable stations in the tiny dataset!)

    # Note that values should be printed in celsius

    # Replace None with an appropriate expression
    # Replace STATION, STATIONNAME, and TAVGDIFF with appropriate expressions

    # print('Top 5 differences:')
    # for row in df_decade_diff_sorted.limit(5).collect():
    #     STATION = row['STATION']
    #     STATIONNAME = row['NAME']
    #     TAVGDIFF = row['DIFF']
    #     print(f'{STATION} at {STATIONNAME} difference {TAVGDIFF:0.1f} °C)')

    # # replace None with an appropriate expression
    # print('Fraction of positive differences:')
    # fraction_positive_diff = df_decade_diff_sorted.filter(col('DIFF') > 0).count() / df_decade_diff_sorted.filter(col('DIFF').isNotNull()).count()
    # print(fraction_positive_diff)

    # Five-number summary of temperature differences, replace with appropriate expressions
    print('Five-number summary of decade average difference values:')
    # Extract the DIFF column
    decade_diff = df_decade_diff_sorted.select('DIFF').collect()
    diff_values = [row['DIFF'] for row in decade_diff if row['DIFF'] is not None]
    print(diff_values)
    nr_rows = len(diff_values)
    tdiff_max = diff_values[0]
    tdiff_min = diff_values[-1]
    tdiff_q1 = diff_values[3 * nr_rows // 4]
    tdiff_median = diff_values[nr_rows // 2]
    tdiff_q3 = diff_values[nr_rows // 4]
    print(f'tdiff_min {tdiff_min:0.1f} °C')
    print(f'tdiff_q1 {tdiff_q1:0.1f} °C')
    print(f'tdiff_median {tdiff_median:0.1f} °C')
    print(f'tdiff_q3 {tdiff_q3:0.1f} °C')
    print(f'tdiff_max {tdiff_max:0.1f} °C')

    # # Add your time measurements here
    # # It may be interesting to also record more fine-grained times (e.g., how 
    # # much time was spent computing vs. reading data)
    # print(f'num workers: {args.num_workers}')
    # print(f'total time: {None:0.1f} s')
