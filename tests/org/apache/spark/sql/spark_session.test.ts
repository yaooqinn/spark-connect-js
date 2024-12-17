import { SparkSession } from "../../../../../src/org/apache/spark/sql/spark_session";
import { DataTypes } from "../../../../../src/org/apache/spark/sql/types/DataTypes";

test("builder", () => {
  const spark = SparkSession
    .builder()
    .remote('sc://localhost')
    .appName('test')
    // change default value from 200 to 2024
    .config('spark.sql.shuffle.partitions', '1024')
    .config('spark.kent.yao', 'awesome')
    .getOrCreate();

    spark.then(s => {
      s.version().then(version => {
        expect(version).toBe("4.0.0-SNAPSHOT");
      });
      s.conf().get("spark.sql.shuffle.partitions").then(value => {
        expect(value).toBe("1024");
      });
      s.conf().getAll().then(configs => {
        expect(configs.get("spark.kent.yao")).toBe("awesome");
      });
      s.sql("SELECT 1 + 1 as a").then(df => {
        df.schema().then(schema => {
          expect(schema).toEqual(DataTypes.createStructType([DataTypes.createStructField("a", DataTypes.IntegerType, false)]));
          expect("spark").toBe("spark2");

        });

      });
    });
});