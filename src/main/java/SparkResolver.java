import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

class SparkResolver {

    private SparkSession sparkSession;

    void provideSparkRows() {
        sparkSession = SparkSession
                .builder()
                .appName("Java Spark CSV Example")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> data = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                .load("src/main/resources/data/10K.github.jsonl");
        data = data.filter("_c1 = '\"type\":\"PushEvent\"' AND _c20 LIKE '%message%'").select("_c3", "_c20");
        data = processRows(data);
        generateResult(data.collectAsList());
    }

    private Dataset<Row> processRows(Dataset<Row> data) {
        sparkSession.udf().register("extractNameAndMessages", (String login, String message) -> {
            String[] words = message.toLowerCase().replaceAll("[^a-z ]", "").split("\\s+");
            List<String> threeGrams = new ArrayList<>();
            for (int i = 0; i < words.length - 2; i++) {
                String threeGram = words[i] + " " + words[i + 1] + " " + words[i + 2];
                threeGrams.add(threeGram);
            }
            StringBuilder result = new StringBuilder(login.split(":")[1].replace("\"", "").trim());
            for (String threeGram : threeGrams) {
                result.append(", ").append(threeGram);
            }
            return result.toString();
        }, DataTypes.StringType);
        data = data.withColumn("name_and_messages", functions.callUDF("extractNameAndMessages",
                data.col("_c3"),
                data.col("_c20")));
        data = data.select(data.col("name_and_messages"));
        return data;
    }

    private void generateResult(List<Row> lines) {
        if (lines == null) {
            throw new IllegalArgumentException("Lines must not be null");
        }
        try (FileWriter writer = new FileWriter("results.txt")) {
            for (Row line : lines) {
                writer.write(line.toString() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
