echo "Generating up-to-date jar of macrobenchmarks..."
#sbt macrobenchmarks/assembly
echo "Generating up-to-date jar of macrobenchmarks...[DONE]"

class="org.apache.spark.graphx.pkgraph.macrobenchmarks.GraphBenchmark"
master="local[4]"
jar="macrobenchmarks/target/scala-2.12/macrobenchmarks-assembly-1.0.0.jar"
outputDir="macrobenchmarks/reports"

algorithm="pageRank"
impl="GraphX"
dataset="uk-2002"

echo "## Benchmarking '${algorithm}' for '${impl}' ###"

spark-submit \
  --class "$class" \
  --master "$master" $jar \
  --implementation "$impl" \
  --algorithm "$algorithm" \
  --input "datasets/${dataset}.mtx" \
  --output "$outputDir/${impl}-${algorithm}-${dataset}-benchmark"

echo "## Benchmarking '${algorithm}' for '${impl}' [DONE] ###"
