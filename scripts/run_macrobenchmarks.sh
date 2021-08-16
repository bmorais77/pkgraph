CLUSTER_ID=$1
aws emr add-steps --cluster-id ${CLUSTER_ID} --steps file://./scripts/macrobenchmark-step.json
