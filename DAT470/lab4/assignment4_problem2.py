import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg
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
        return pd.DataFrame([])
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
    
    # Start the timer
    start_time = time.time()

    # read the CSV file into a pyspark.sql dataframe and compute the things you need
    df = spark.read.csv(args.filename, header=True, inferSchema=True)

    # Add Julian date number, average temperature (in F and C), and decade to the dataframe
    df = df.withColumn('JDN', jdn(col('DATE'))) \
        .withColumn('TAVG', (col('TMAX') + col('TMIN')) / 2.0) \
        .withColumn('TAVG_C', (col('TAVG') - 32) * 5.0 / 9) \
        .withColumn('DECADE', decade(col('DATE')))

    # Cache the dataframe to speed up computations
    df = df.cache()

    # For each station, performs fits a simple linear regression model using 
    # least squares (T_avg as function of JDN)
    pd_lsq = df.groupBy('STATION').applyInPandas(lsq, schema='STATION STRING, ALPHA DOUBLE, BETA DOUBLE')

    # Add the station name to the dataframe
    station_names = df.select('STATION', 'NAME').dropDuplicates(['STATION']).cache()
    pd_lsq_named = pd_lsq.join(station_names, on='STATION', how='left')
    pd_lsq_named = pd_lsq_named.cache()

    # top 5 slopes are printed here
    print('Top 5 coefficients:')
    t_1 = time.time()
    top_5 = pd_lsq_named.orderBy(col('BETA').desc()).limit(5).collect()
    t_2 = time.time()
    for row in top_5:
        STATIONCODE = row['STATION']
        STATIONNAME = row['NAME']
        BETA = row['BETA']
        print(f'{STATIONCODE} at {STATIONNAME} BETA={BETA:0.3e} °F/d')

    print('Fraction of positive coefficients:')
    t_3 = time.time()
    fraction_positive = pd_lsq_named.filter(col('BETA') > 0).count() / pd_lsq_named.count()
    t_4 = time.time()
    print(fraction_positive)

    # Five-number summary of slopes
    print('Five-number summary of BETA values:')
    five_num = pd_lsq_named.approxQuantile('BETA', [0.0, 0.25, 0.5, 0.75, 1.0], 0.0)
    print(f'beta_min {five_num[0]:0.3e}')
    print(f'beta_q1 {five_num[1]:0.3e}')
    print(f'beta_median {five_num[2]:0.3e}')
    print(f'beta_q3 {five_num[3]:0.3e}')
    print(f'beta_max {five_num[4]:0.3e}')

    # Here you will need to implement computing the decadewise differences 
    # between the average temperatures of 1910s and 2010s

    df_decade_temp = df.groupBy('STATION', 'DECADE') \
        .agg(avg('TAVG').alias('TAVG'))
    # Compute the difference between the average temperature of the 2010s and the 1910s
    df_decade_diff = df_decade_temp.groupBy('STATION') \
        .applyInPandas(compute_decade_diff, schema='STATION STRING, DIFF DOUBLE')
    
    df_decade_diff = df_decade_diff.cache()
    # Add the station name to the dataframe
    df_decade_diff = df_decade_diff.join(station_names, on='STATION', how='left')

    # Check if any suitable values were computed
    t_5 = time.time()
    nr_of_decades = df_decade_diff.count()
    t_6 = time.time()
    if nr_of_decades == 0:
        print('No suitable stations found for computing the decadewise differences.')
    else:
        print('Top 5 differences:')
        t_7 = time.time()
        # Sort by the difference in descending order
        top_5_diff = df_decade_diff.orderBy(col('DIFF').desc()).limit(5).collect()
        t_8 = time.time()
        for row in top_5_diff:
            STATION = row['STATION']
            STATIONNAME = row['NAME']
            TAVGDIFF = row['DIFF']
            print(f'{STATION} at {STATIONNAME} difference {TAVGDIFF * 5.0 / 9:0.1f} °C)')

        print('Fraction of positive differences:')
        t_9 = time.time()
        fraction_positive_diff = df_decade_diff.filter(col('DIFF') > 0).count() / df_decade_diff.count()
        t_10 = time.time()
        print(fraction_positive_diff)

        # Five-number summary of temperature differences, replace with appropriate expressions
        print('Five-number summary of decade average difference values:')
        five_num = df_decade_diff.approxQuantile('DIFF', [0.0, 0.25, 0.5, 0.75, 1.0], 0.0)
        for i in range(len(five_num)):
            five_num[i] = five_num[i] * 5.0 / 9
        print(f'tdiff_min {five_num[0]:0.1f} °C')
        print(f'tdiff_q1 {five_num[1]:0.1f} °C')
        print(f'tdiff_median {five_num[2]:0.1f} °C')
        print(f'tdiff_q3 {five_num[3]:0.1f} °C')
        print(f'tdiff_max {five_num[4]:0.1f} °C')

    # Add your time measurements here
    time_total = time.time() - start_time
    time_computations = t_2 - t_1 + t_4 - t_3 + t_6 - t_5 + t_8 - t_7 + t_10 - t_9
    time_reading = time_total - time_computations
    print(f'num workers: {args.num_workers}')
    print(f'total time: {time_total:0.1f} s')
    print(f'time reading: {time_reading:0.1f} s')
    print(f'time computations: {time_computations:0.1f} s')

