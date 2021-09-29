INSTANCE_COUNT=$1

aws emr create-cluster \
  --name "Graph Benchmark" \
  --release-label emr-6.3.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count ${INSTANCE_COUNT} \
  --use-default-roles \
  --auto-terminate \
  --log-uri s3://pkgraph-storage/logs/ \
  --steps file://./scripts/macrobenchmark-step.json
