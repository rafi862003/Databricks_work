class common_utils:
    def __init__(self,filepath):
        from pyspark.sql import SparkSession
        self.spark = SparkSession.builder.getOrCreate()
        self.filepath = filepath

    def df_count_rows_cols(self):
        df = self.spark.read.csv(self.filepath, header=True)
        return {"columns": len(df.columns), "rows": df.count()}
    
    def df_read_file_csv(self):
        df=self.spark.read.csv(self.filepath, header=True)
        return df.display()

#X=common_utils('/Volumes/workspace/default/my_files/hflights_2.csv')
#X.df_count_rows_cols()
