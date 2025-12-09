from databricks.connect import DatabricksSession

from dbx_repro.main import callRestAPIBatch


def test_foreachbatch():
    spark = DatabricksSession.builder.getOrCreate()
    spark.addArtifact("./src", pyfile=True)

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
