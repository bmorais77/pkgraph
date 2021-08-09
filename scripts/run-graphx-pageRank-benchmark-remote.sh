echo "Generating up-to-date jar of macrobenchmarks..."
#sbt macrobenchmarks/assembly
echo "Generating up-to-date jar of macrobenchmarks...[DONE]"

gcloud dataproc jobs submit spark \
  --cluster=pkgraph-cluster \
  --region=europe-west2 \
  --jars=macrobenchmarks/target/scala-2.12/macrobenchmarks-assembly-1.0.0.jar \
  --class=org.apache.spark.graphx.pkgraph.macrobenchmarks.GraphBenchmark \
  --  \
  --implementation GraphX \
  --algorithm pageRank \
  --input gs://pkgraph-storage/uk-2002.mtx \
  --output macrobenchmarks/reports/graphx-pageRank-uk-2002-benchmark
