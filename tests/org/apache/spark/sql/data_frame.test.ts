import { SparkSession } from '../../../../../src/org/apache/spark/sql/spark_session';

test("abc", async () => {
  const spark = await SparkSession
    .builder()
    .appName('test')
    .config('spark.sql.shuffle.partitions', '2')
    .getOrCreate();

  // const df = await spark.sql("SELECT 1 + 1 as a");

  // const schema = await df.schema();
  // expect(schema).toBe("a");
}, 30000);