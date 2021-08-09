import argparse
import re
import sys
import subprocess
import time

from enum import Enum
from typing import Any, List

mainClass="org.apache.spark.graphx.pkgraph.macrobenchmarks.GraphBenchmark"
jar="macrobenchmarks/target/scala-2.12/macrobenchmarks-assembly-1.0.0.jar"

outputDir="macrobenchmarks/reports"
logDir="macrobenchmarks/logs"

class BenchmarkMode(Enum):
    Local: str = 'local'
    Remote: str = 'remote'

    def __str__(self) -> str:
        return self.value

def parse_args() -> Any:
    parser = argparse.ArgumentParser(description="Tool to automatically run local and remote macrobenchmarks")
    parser.add_argument("Mode", type=BenchmarkMode, choices=list(BenchmarkMode), help="Benchmark mode to use")
    parser.add_argument("--BuildJar", action="store_true", help="Flag to build the benchmark jar before executing the benchmarks")
    parser.add_argument("--IFilter", type=str, help="Regex expression to filter implementations tested in the benchmark")
    parser.add_argument("--AFilter", type=str, help="Regex expression to filter algorithms tested in the benchmark")
    parser.add_argument("--DFilter", type=str, help="Regex expression to filter datasets used in the benchmark")
    return parser.parse_args()

def printProgressBar (iteration, total, currBenchmark = ""):
    length = 50
    percent = ("{0:.1f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = '█' * filledLength + '-' * (length - filledLength)
    suffix = f"(Running: {currentBenchmark})" if currBenchmark else ""
    print(f'\rProgress |{bar}| {percent}% Complete ({iteration}/{total}) {suffix}', end = "\r")  

def runLocalBenchmark(algorithm: str, implementation: str, dataset: str) -> int:
    returnCode = 0
    with open(f"{logDir}/graph-benchmark-{algorithm}-{implementation}-{dataset}.log", "w+") as outFile:
        spark = subprocess.run([
            "spark-submit",
            "--class", mainClass,
            "--master", "local[*]", jar,
            "--implementation", implementation,
            "--algorithm", algorithm,
            "--input", f"datasets/{dataset}.mtx",
            "--output", f"{outputDir}/{impl}-{algorithm}-{dataset}-benchmark"
            ], stdout=outFile, stderr=outFile)
        returnCode = spark.returncode

    return returnCode

def runRemoteBenchmark(algorithm: str, implementation: str, dataset: str) -> int:
    return 0

def runBenchmark(mode: BenchmarkMode, algorithm: str, implementation: str, dataset: str) -> int:
    if(mode == BenchmarkMode.Local):
        return runLocalBenchmark(algorithm, implementation, dataset)
    elif(mode == BenchmarkMode.Remote):
        return runRemoteBenchmark(algorithm, implementation, dataset)
    else:
        raise Exception(f"unknown benchmark mode: {mode}")


args = parse_args()
mode: BenchmarkMode = args.Mode
iFilter: str = args.IFilter if args.IFilter else ".*"
aFilter: str = args.AFilter if args.AFilter else ".*"
dFilter: str = args.DFilter if args.DFilter else ".*"

implementations: List[str] = ["GraphX", "PKGraph2", "PKGraph4", "PKGraph8"]
algorithms: List[str] = ["pageRank"]
datasets: List[str] = ["uk-2002"]

iFilterRegex = re.compile(iFilter)
aFilterRegex = re.compile(aFilter)
dFilterRegex = re.compile(dFilter)

# Apply filters
implementations = [impl for impl in implementations if iFilterRegex.search(impl)]
algorithms = [alg for alg in algorithms if aFilterRegex.search(alg)]
datasets = [data for data in datasets if dFilterRegex.search(data)]

totalBenchmarks = len(implementations) * len(algorithms) * len(datasets)
header = f"""
#{'-' * 100}#
# Macrobenchmarks
# - Mode: {mode}
# - IFilter: {iFilter}
# - AFilter: {aFilter}
# - DFilter: {dFilter}
# 
# - Implementations: {implementations}
# - Algorithms: {algorithms}
# - Datasets: {datasets}
# 
# - Benchmark Count: {totalBenchmarks}
#{'-' * 100}#
"""

print(header)

if args.BuildJar:
    print("Generating up-to-date macrobenchmarks jar...")
    sbt = subprocess.run(["sbt", "macrobenchmarks/assembly"], stdout=subprocess.DEVNULL)
    if(sbt.returncode != 0):
        raise Exception("failed to generated up-to-date macrobenchmarks jar. Make sure this script is executed from the root of the project.")
    print("Generated up-to-date macrobenchmarks jar")

print("Running Benchmarks...")

i = 0
failures = 0
start = time.time()

for alg in algorithms:
    for impl in implementations:
        for dataset in datasets:
            currentBenchmark = f"{alg}/{impl}/{dataset}"
            printProgressBar(i, totalBenchmarks, currBenchmark=currentBenchmark)

            exitCode = runBenchmark(mode, alg, impl, dataset)
            if exitCode != 0:
                failures += 1

            i += 1
            pass

sys.stdout.write("\033[K") # Clear to the end of line
printProgressBar(i, totalBenchmarks)
print()

end = time.time()
duration = end - start

footer = f"""
#{'-' * 100}#
# Macrobenchmarks Completed
# - Benchmark Count: {i}
# - Failures: {failures}
# - Duration: {duration}
#{'-' * 100}#
"""
print(footer)