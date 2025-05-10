import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, StructType, StructField, DoubleType, StringType
import pandas as pd
from math import floor
import sys

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
def lsq(key,df):
    y = df["TAVG"]
    x = df["JudianDate"]

    x_mean = x.mean()
    y_mean = y.mean()

    deltaX = x - x_mean
    deltaY = y - y_mean

    beta = (deltaX * deltaY).sum() / (deltaX ** 2).sum()
    alpha = y_mean - beta * x_mean

    return pd.DataFrame({"STATION": key, "alpha": alpha,"beta": beta})

def decAvg(key,df):
    x = df["TAVG"]
    name = df["NAME"].iloc[0]
    x_count = len(x)
    x_sum = x.sum()
    tAvg = x_sum/x_count
    return pd.DataFrame({"STATION": key,"NAME": name, "TAVG": tAvg})

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
    start = time.time()
    df = spark.read.csv(args.filename, header=True, inferSchema = True)

    startComputation = time.time()

    #Computes the Julian date number for each record
    df = df.withColumn("JudianDate", jdn(col("DATE")))
    
    #Computes the T average number for each record
    df = df.withColumn("TAVG", (col("TMIN") + col("TMAX")) / 2)

    df = df.withColumn("TAVG", (col("TAVG") - 32) * 5 / 9) #convert to celsius

    # For each station, performs fits a simple linear regression model 
    # using least squares (Tavg as function of JDN)

    regressionSchema = StructType([
    StructField("STATION", StringType()),
    StructField("alpha", DoubleType()),
    StructField("beta", DoubleType())
    ])
    

    regressionDf = df.groupBy("STATION").applyInPandas(lsq, regressionSchema)
    regressionDf = regressionDf.join(
        df.select("STATION", "NAME").distinct(),
        on="STATION",
        how="left"
    )

    top5 = regressionDf.orderBy(col("beta").desc()).limit(5)
    rows = top5.collect()

    # Sort by beta in descending order
    betas = regressionDf.orderBy(col("beta").desc()).select("beta").collect()

    betas = [row["beta"] for row in betas]
    betas_count = len(betas)

    beta_max = betas[0]
    beta_min = betas[-1]

    beta_median = betas[floor(betas_count / 2)]

    beta_q1 = betas[floor(betas_count * 3 / 4)] 
    beta_q3 = betas[floor(betas_count / 4)] 

    negative_betas = 0
    positive_betas = 0
    for b in betas:
        if b >= 0:
            positive_betas += 1
        if b < 0:
            negative_betas += 1 

    # top 5 slopes are printed here
    # replace None with your dataframe, list, or an appropriate expression
    # replace STATIONCODE, STATIONNAME, and BETA with appropriate expressions
    print('Top 5 coefficients:')
    for row in rows:
        print(f'{row["STATION"]} at {row["NAME"]} BETA={row["beta"]:0.3e} °F/d')

    # replace None with an appropriate expression
    print('Fraction of positive coefficients:')
    print(f'{positive_betas/betas_count}')

    # Five-number summary of slopes, replace with appropriate expressions
    print('Five-number summary of BETA values:')
    print(f'beta_min {beta_min:0.3e}')
    print(f'beta_q1 {beta_q1:0.3e}')
    print(f'beta_median {beta_median:0.3e}')
    print(f'beta_q3 {beta_q3:0.3e}')
    print(f'beta_max {beta_max:0.3e}')

    # Here you will need to implement computing the decadewise differences 
    # between the average temperatures of 1910s and 2010s

    df_1910s = df.filter((col("JudianDate") > 2418673) & (col("JudianDate") < 2422325))
    df_2010s = df.filter((col("JudianDate") > 2455198) & (col("JudianDate") < 2458850))

    if(df_1910s.count() <= 0):
        print("No data from 1910!")
        raise RuntimeError("NO DATA FROM 1910s")
    
    tAvgSchema = StructType([
    StructField("STATION", StringType()),
    StructField("NAME", StringType()),
    StructField("TAVG", DoubleType())
    ])
    
    tAvg1910sDf = df_1910s.groupBy("STATION").applyInPandas(decAvg, tAvgSchema)
    tAvg2010sDf = df_2010s.groupBy("STATION").applyInPandas(decAvg, tAvgSchema)

    tAvg1910sDf_ = tAvg1910sDf.withColumnRenamed("TAVG", "1910TAVG")
    tAvg2010sDf_ = tAvg2010sDf.withColumnRenamed("TAVG", "2010TAVG")

    merged = tAvg1910sDf_.join(tAvg2010sDf_, on="STATION", how="inner")

    tAvgDiff = merged.withColumn("TAVG_diff", col("2010TAVG") - col("1910TAVG"))

    tAvgDiff = tAvgDiff.withColumn("TAVG_diff", col("TAVG_diff"))

    top5Diff = tAvgDiff.orderBy(col("TAVG_diff").desc()).limit(5)
    rowsDiff = top5Diff.collect()


    diffs = tAvgDiff.orderBy(col("TAVG_diff").desc()).select("TAVG_diff").collect()

    diffs = [diff["TAVG_diff"] for diff in diffs]
    diffs_count = len(diffs)

    tdiff_max = diffs[0]
    tdiff_min = diffs[-1]

    tdiff_median = diffs[floor(diffs_count / 2)]

    tdiff_q1 = diffs[floor(diffs_count * 3 / 4)] 
    tdiff_q3 = diffs[floor(diffs_count / 4)] 

    negative_diffs = 0
    positive_diffs = 0
    for d in diffs:
        if d >= 0:
            positive_diffs += 1
        if d < 0:
            negative_diffs += 1 
    # There should probably be an if statement to check if any such values were 
    # computed (no suitable stations in the tiny dataset!)

    # Note that values should be printed in celsius

    # Replace None with an appropriate expression
    # Replace STATION, STATIONNAME, and TAVGDIFF with appropriate expressions
    end = time.time()
    total_time = end - start
    total_computation_time = end - startComputation
    print('Top 5 differences:')
    for row in rowsDiff:
        print(f'{row["STATION"]} at {row["NAME"]} difference {row["TAVG_diff"]:0.1f} °C)')

    # replace None with an appropriate expression
    print('Fraction of positive differences:')
    print(f'{positive_diffs/diffs_count}')

    # Five-number summary of temperature differences, replace with appropriate expressions
    print('Five-number summary of decade average difference values:')
    print(f'tdiff_min {tdiff_min:0.1f} °C')
    print(f'tdiff_q1 {tdiff_q1:0.1f} °C')
    print(f'tdiff_median {tdiff_median:0.1f} °C')
    print(f'tdiff_q3 {tdiff_q3:0.1f} °C')
    print(f'tdiff_max {tdiff_max:0.1f} °C')

    # Add your time measurements here
    # It may be interesting to also record more fine-grained times (e.g., how 
    # much time was spent computing vs. reading data)
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time} s')
    print(f'total computation time: {total_computation_time} s')
    print(f'total reading time: {total_time - total_computation_time} s')
