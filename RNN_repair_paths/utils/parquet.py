import pyarrow as pa
import pyarrow.parquet as pq
import dask
import dask.dataframe as dd
import traceback
import glob

class Parquet(object):

    def writeToFile(self, df, filename):
        try:
            table = pa.Table.from_pandas(df, preserve_index=True)
            pq.write_table(table, filename)
        except Exception as e:
            print(traceback.format_exc())


    def readSequenceOfFiles(self, location, prefix):
        try:
            files = glob.glob(f"{location}{prefix}*")
            return dd.read_parquet(files).compute()
        except Exception as e:
            print(traceback.format_exc())


    def readSingleFile(self, filename):
        try:
            return dd.read_parquet(filename).compute()
        except Exception as e:
            print(traceback.format_exc())
