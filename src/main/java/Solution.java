import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static scala.jdk.CollectionConverters.IterableHasAsScala;

record Input(long number, String company, long value, int partition) implements Serializable {
}

record Output(long number, String company, long value, long prevNumber, long prevValue,
              int partition) implements Serializable {
}

class IndexAwareDecodingIterator implements Iterator<Input>, Serializable {
    final Iterator<Row> baseIterator;
    final Integer partitionIndex;

    public IndexAwareDecodingIterator(Iterator<Row> baseIterator, Integer partitionIndex) {
        this.baseIterator = baseIterator;
        this.partitionIndex = partitionIndex;
    }

    @Override
    public boolean hasNext() {
        return baseIterator.hasNext();
    }

    @Override
    public Input next() {
        var row = baseIterator.next();

        return new Input(row.getLong(0), row.getString(1), row.getLong(2), this.partitionIndex);
    }
}

public class Solution {
    /*
    To make it possible to use Spark in current form in Java 17, following VM flags were used:

    --add-opens=java.base/java.lang=ALL-UNNAMED
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    --add-opens=java.base/java.io=ALL-UNNAMED
    --add-opens=java.base/java.net=ALL-UNNAMED
    --add-opens=java.base/java.nio=ALL-UNNAMED
    --add-opens=java.base/java.util=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
    --add-opens=java.base/sun.security.action=ALL-UNNAMED
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED

    This opens questions regarding security, but they're not necessarily all that severe, considering that
    the restrictions were introduced only recently.
    * */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        var spark = SparkSession
            .builder()
            .master("local")
            .getOrCreate();
        var solution = new Solution();

        solution.solve(spark, "inputs/", "outputs/");
//        solution.solveWithDf(spark, "inputs/", "outputs/");
    }

    public void solve(SparkSession sparkSession, String inputDirectory, String outputDirectory) {
        var inputSchema = new StructType(
            new StructField[]{
                StructField.apply("Number", LongType, false, Metadata.empty()),
                StructField.apply("Company", StringType, false, Metadata.empty()),
                StructField.apply("Value", LongType, false, Metadata.empty())
            }
        );

        var csvDf = sparkSession
            .read()
            .schema(inputSchema)
            .csv(inputDirectory)
            .withColumn(
                "partition",
                regexp_extract(input_file_name(), "input-(\\d+)[.]csv", 1).cast(IntegerType)
//                spark_partition_id().as("partition")
        );

        // to simulate input partitioning
//        var repartitionedDf = renamedDf.repartition(2, floor(col("Number").minus(1).divide(4)));
        var repartitionedDf = csvDf.repartition(2, col("partition"));

        var rowRdd = repartitionedDf.toJavaRDD();

        // This was an attempt to obtain the partition index for each row, using the class defined above Solution.
        // It didn't seem to work as I'd like it to, consistently returning '1', it being the index of the second partition.
//        var inputRecordRdd = rowRdd.mapPartitionsWithIndex(
//            (index, inputs) -> new IndexAwareDecodingIterator(inputs, index), false
//        );

        var inputRecordRdd = rowRdd.map(
                (row) -> new Input(
                        row.getLong(0),
                        row.getString(1),
                        row.getLong(2),
                        row.getInt(3)
                )
        );

        var sortedRdd = inputRecordRdd.sortBy(Input::number, true, inputRecordRdd.getNumPartitions());

        // In all likelihood, the performance/nonfunctional constraints aren't solved in expected way. Due to working with an iterator,
        // I assume that approach using .mapPartitions would be better, but I haven't found a satisfactory way in the API
        // to merge the results between partitions, so I proceeded with the current approach to have something that works,
        // and used what's left of the time limit to try to finalize it.
        var processedRdd = sortedRdd
            .keyBy(Input::company)
            .combineByKey(
                input -> List.of(new Output(input.number(), input.company(), input.value(), 0, 0, input.partition())),
                (output, input) -> {
                    var lastElem = output.get(output.size() - 1);

                    var prevValue = 0L;
                    var prevNumber = 0L;

                    if (lastElem.value() > 1000) {
                        prevNumber = lastElem.number();
                        prevValue = lastElem.value();
                    } else {
                        prevNumber = lastElem.prevNumber();
                        prevValue = lastElem.prevValue();
                    }

                    var newElem = new Output(input.number(), input.company(), input.value(), prevNumber, prevValue, input.partition());

                    return Stream.concat(output.stream(), Stream.of(newElem)).toList();
                },
                (left, right) -> {
                    // TODO deduplicate
                    var lastElem = left.get(left.size() - 1);
                    var prevValue = 0L;
                    var prevNumber = 0L;

                    if (lastElem.value() > 1000) {
                        prevNumber = lastElem.number();
                        prevValue = lastElem.value();
                    } else {
                        prevNumber = lastElem.prevNumber();
                        prevValue = lastElem.prevValue();
                    }

                    // Propagate - that is:
                    // The `left` list should be already correct; the last PrevValue of it should be used for all the elements
                    // in `right` until a non-zero one is found
                    long finalPrevNumber = prevNumber;
                    long finalPrevValue = prevValue;

                    // this would be better if there was a split at the point where prevNumber stops being zero
                    var newRight= right.stream().map(out -> {
                        if (out.prevNumber() == 0)
                            return new Output(out.number(), out.company(), out.value(), finalPrevNumber, finalPrevValue, out.partition());
                        else return out;
                    });

                    return Stream.concat(left.stream(), newRight).toList();
                }
            )
            .values()
            .flatMap(List::listIterator)
            .sortBy(Output::number, true, inputRecordRdd.getNumPartitions());

        // back from RDD to DataFrame for saving purposes
        var outputRowRdd = processedRdd.map(
            output -> Row.fromSeq(
                IterableHasAsScala(List.<Object>of(
                    output.number(),
                    output.company(),
                    output.value(),
                    output.prevNumber(),
                    output.prevValue(),
                    output.partition()
                )).asScala().toSeq()
            )
        );

        var schema = new StructType(
            new StructField[]{
                StructField.apply("Number", LongType, false, Metadata.empty()),
                StructField.apply("Company", StringType, false, Metadata.empty()),
                StructField.apply("Value", LongType, false, Metadata.empty()),
                StructField.apply("PrevNumber", LongType, false, Metadata.empty()),
                StructField.apply("PrevValue", LongType, false, Metadata.empty()),
                StructField.apply("partition", IntegerType, false, Metadata.empty())
            }
        );

        var outDf = sparkSession
            .createDataFrame(outputRowRdd, schema)
            .repartition(repartitionedDf.rdd().getNumPartitions(), col("partition"));
//            .drop("partition");

        // Likely won't address writing the output by the given filenames and structure, because of the time constraints.
        // This could be achieved by either customizing the writer behavior, or moving the resulting files afterwards
        outDf.write().format("csv").partitionBy("partition").mode(SaveMode.Overwrite).save(outputDirectory);

    }

    // First attempted solving the problem with fully DF based approach, so as to have some baseline to work with.
    // Does not deal with partitioning and the like, other than the join at the end trying to preserve the structure,
    // based on the vague notion that the output partitions will be the same as in the left-hand-side of the join.
    public void solveWithDf(SparkSession sparkSession, String inputDirectory, String outputDirectory) {
        var inputSchema = new StructType(
                new StructField[]{
                        StructField.apply("Number", LongType, false, Metadata.empty()),
                        StructField.apply("Company", StringType, false, Metadata.empty()),
                        StructField.apply("Value", LongType, false, Metadata.empty())
                }
        );

        var csvDf = sparkSession.read().schema(inputSchema).csv(inputDirectory);

        var window = Window
                .partitionBy("company")
                .orderBy("number")
                .rowsBetween(Window.unboundedPreceding(), -1);

        var rightDf = csvDf
                .withColumn(
                        "conditionallyNullableNumber",
                        when(col("Value").leq(1000), lit(null)).otherwise(col("Number"))
                )
                .withColumn(
                        "conditionallyNullableValue",
                        when(col("Value").leq(1000), lit(null)).otherwise(col("Value"))
                )
                .select(
                        col("Number"),
                        coalesce(last(col("conditionallyNullableNumber"), true).over(window), lit(0)).as("PrevNumber"),
                        coalesce(last(col("conditionallyNullableValue"), true).over(window), lit(0)).as("PrevValue")
                );

        var resultDf = csvDf
                .join(rightDf, IterableHasAsScala(List.of("number")).asScala().toSeq(), "left")
                .select(
                        col("Number"),
                        col("Value"),
                        col("Company"),
                        coalesce(col("PrevNumber"), lit(0)).as("PrevNumber"),
                        coalesce(col("PrevValue"), lit(0)).as("PrevValue")
                );

        resultDf.show(8, false);
//        resultDf.write().format("csv").mode(SaveMode.Overwrite).save(outputDirectory);

    }
}
