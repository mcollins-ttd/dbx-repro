from databricks.connect import DatabricksSession


def callRestAPIBatch(df, batchId):
    print("ok")


def test_foreachbatch():
    spark = DatabricksSession.builder.getOrCreate()

    dfSource = (spark.readStream
                .format("delta")
                .table("samples.nyctaxi.trips"))

    streamHandle = (dfSource.writeStream
                    .foreachBatch(callRestAPIBatch)
                    .trigger(availableNow=True)
                    .start())

    streamHandle.awaitTermination()


if __name__ == "__main__":
    print("Running...")
    test_foreachbatch()
    print("Done")
