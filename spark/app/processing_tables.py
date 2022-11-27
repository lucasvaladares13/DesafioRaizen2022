
import pathlib
import sys
import os
# Setup Spark
from pyspark.sql import SparkSession

from pyspark.sql.functions import when, regexp_replace, lit,format_string,current_timestamp,to_date


trust_path = sys.argv[1]
refine_path = sys.argv[2]


spark = SparkSession.builder.getOrCreate()
#spark.conf.set("spark.sql.parquet.enableVectorizedReader","false").appName("AtividadeSQL")

def unpivot_table(path):
    df = spark.read.csv(path, inferSchema=True, header=True)
    #df.write.parquet(refine_path,mode = "overwrite")
    list_columns = df.columns
    list_remove = ['Dados','product','unit','uf']
    list_columns = list(set(list_columns) - set(list_remove))
    
    for col in list_columns:
        table_name = f'dt_{col}'
        spark.catalog.dropTempView(table_name)
        
        df.createTempView(table_name)
        df2 = spark.sql(f"select Dados AS month,Dados AS month_num, uf, product, unit, {col} AS volume  from {table_name} where {col} IS NOT NULL")
        year = col.split('_')[-1]
        #print(year)
        df2 = df2.withColumn('month_num', 
        when(df2.month_num.endswith('JANEIRO'),regexp_replace(df2.month_num,'JANEIRO','01')) \
        .when(df2.month_num.endswith('FEVEREIRO'),regexp_replace(df2.month_num,'FEVEREIRO','02')) \
        .when(df2.month_num.endswith('MARÇO'),regexp_replace(df2.month_num,'MARÇO','03')) \
        .when(df2.month_num.endswith('ABRIL'),regexp_replace(df2.month_num,'ABRIL','04')) \
        .when(df2.month_num.endswith('MAIO'),regexp_replace(df2.month_num,'MAIO','05')) \
        .when(df2.month_num.endswith('JUNHO'),regexp_replace(df2.month_num,'JUNHO','06')) \
        .when(df2.month_num.endswith('JULHO'),regexp_replace(df2.month_num,'JULHO','07')) \
        .when(df2.month_num.endswith('AGOSTO'),regexp_replace(df2.month_num,'AGOSTO','08')) \
        .when(df2.month_num.endswith('SETEMBRO'),regexp_replace(df2.month_num,'SETEMBRO','09')) \
        .when(df2.month_num.endswith('OUTUBRO'),regexp_replace(df2.month_num,'OUTUBRO','10')) \
        .when(df2.month_num.endswith('NOVEMBRO'),regexp_replace(df2.month_num,'NOVEMBRO','11')) \
        .when(df2.month_num.endswith('DEZEMBRO'),regexp_replace(df2.month_num,'DEZEMBRO','12')) \
        .otherwise(df2.month_num))
        df2 =df2.select('*', lit(year).alias("year"))
        df2 =df2.select('*', current_timestamp().alias("created_at"))
        df2 = df2.withColumn('year_month',format_string("%s/%s",df2.year,df2.month_num))
        df2 = df2.withColumn('year_month',to_date(df2.year_month, "yyyy/MM"))
        df2 = df2.select('year_month','uf','product','unit','volume','created_at')

        try: 
            df_export = df_export.unionAll(df2)
        
        except:
            df_export = df2

    #df2.show()
    spark.catalog.dropTempView(table_name)

    #df_export.show()



    return df_export

def join_csv(path_folder):

    list_files = list(pathlib.Path(path_folder).glob('**/*.csv'))
    for file in list_files:
        file = str(file)
        print(str(file))
        df = unpivot_table(file)

        try:
            df_export = df_export.unionAll(df)
        except:
            df_export = df

    return df_export

df = join_csv(trust_path)

df.show(1000)

#df.write.format("parquet").partitionBy('year_month').mode('overwrite').save(refine_path)

spark.stop()


