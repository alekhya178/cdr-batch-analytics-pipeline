import argparse
import json
import os
import math
from datetime import datetime, timezone
from pyspark.sql import SparkSession  # pyrefly: ignore [missing-import]
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType  # pyrefly: ignore [missing-import]

def write_hdfs_file(spark, path, content):
    try:
        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        hdfs_path = Path(path)
        fs = hdfs_path.getFileSystem(hadoop_conf)
        out = fs.create(hdfs_path, True)
        out.write(bytearray(content, 'utf-8'))
        out.close()
        print(f"Successfully wrote HDFS file: {path}")
    except Exception as e:
        print(f"Error writing to HDFS path {path}: {e}")


def caller_partitioner(key):
    import hashlib
    # Return a stable partition index using MD5 hash of caller_id string
    # We will let Spark modulo the returned value, but return a hash int
    h = hashlib.md5(key.encode('utf-8')).hexdigest()
    return int(h, 16)

def process_partition(iterator):
    # Group records by caller_id in this partition
    from collections import defaultdict
    import math
    
    caller_records = defaultdict(list)
    for caller_id, row in iterator:
        caller_records[caller_id].append(row)
        
    anomalies = []
    for caller_id, records in caller_records.items():
        n = len(records)
        if n < 2:
            # Cannot calculate standard deviation with < 2 records
            continue
            
        durations = [float(r['duration_sec']) for r in records]
        mean = sum(durations) / n
        
        # Sample variance and standard deviation
        variance = sum((x - mean) ** 2 for x in durations) / (n - 1)
        stddev = math.sqrt(variance)
        
        if stddev == 0:
            continue
            
        # Detect anomalies > 3σ
        for r in records:
            dur = float(r['duration_sec'])
            if abs(dur - mean) > 3 * stddev:
                anomalies.append((
                    caller_id,
                    str(r['timestamp']),
                    int(dur),
                    float(mean),
                    float(stddev)
                ))
    return anomalies

def main():
    parser = argparse.ArgumentParser(description="Anomalous call duration detection")
    parser.add_argument("--input", default="/data/cdr_data.csv", help="Input CDR file path")
    parser.add_argument("--output-dir", default="/output/anomalous_call_detection", help="Base output directory")
    parser.add_argument("--run-id", required=True, help="Execution run ID")
    parser.add_argument("--num-partitions", type=int, default=16, help="Number of custom partitions")
    args = parser.parse_args()

    job_name = "anomalous_call_detection"
    run_id = args.run_id
    num_partitions = args.num_partitions
    
    # Paths
    hdfs_base_out = f"hdfs://namenode:9000/output/anomalous_call_detection/{run_id}"
    local_base_out = f"{args.output_dir}/{run_id}"
    
    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()
        
    status = "SUCCESS"
    input_count = 0
    output_count = 0
    
    try:
        # Read CDR CSV
        df = spark.read.csv(args.input, header=True, inferSchema=True)
        input_count = df.count()
        
        # Convert to RDD of (caller_id, row_dict)
        rdd = df.rdd.map(lambda r: (r.caller_id, r.asDict()))
        
        # Partition using custom partitioner
        partitioned_rdd = rdd.partitionBy(num_partitions, caller_partitioner)
        
        # Process partitions to detect anomalies
        anomalies_rdd = partitioned_rdd.mapPartitions(process_partition)
        
        # Define output schema
        schema = StructType([
            StructField("caller_id", StringType(), False),
            StructField("call_timestamp", StringType(), False),
            StructField("duration_sec", IntegerType(), False),
            StructField("user_mean_duration", FloatType(), False),
            StructField("user_stddev", FloatType(), False)
        ])
        
        # Convert back to DataFrame
        anomalies_df = spark.createDataFrame(anomalies_rdd, schema)
        output_count = anomalies_df.count()
        
        # Write to HDFS
        anomalies_df.coalesce(1).write.mode("overwrite").csv(hdfs_base_out, header=False)
        
        # Write to local filesystem using python standard library to bypass mount rename issues
        os.makedirs(local_base_out, exist_ok=True)
        local_csv_path = os.path.join(local_base_out, "part-00000.csv")
        local_data = anomalies_df.collect()
        with open(local_csv_path, "w", encoding="utf-8") as f:
            for row in local_data:
                f.write(f"{row['caller_id']},{row['call_timestamp']},{row['duration_sec']},{row['user_mean_duration']},{row['user_stddev']}\n")
        
        print(f"Job completed successfully. Input count: {input_count}, Anomalous count: {output_count}")
        
    except Exception as e:
        status = "FAILURE"
        print(f"Job failed with error: {e}")
        raise e
    finally:
        # Create manifest
        manifest = {
            "job_name": job_name,
            "run_id": run_id,
            "execution_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "input_path": args.input,
            "output_path": f"/output/anomalous_call_detection/{run_id}/",
            "input_record_count": input_count,
            "output_record_count": output_count,
            "status": status
        }
        manifest_str = json.dumps(manifest, indent=2)
        
        # Write local manifest
        os.makedirs(local_base_out, exist_ok=True)
        local_manifest_path = os.path.join(local_base_out, "_MANIFEST.json")
        with open(local_manifest_path, "w") as f:
            f.write(manifest_str)
        print(f"Successfully wrote local manifest: {local_manifest_path}")
            
        # Write HDFS manifest
        hdfs_manifest_path = f"{hdfs_base_out}/_MANIFEST.json"
        write_hdfs_file(spark, hdfs_manifest_path, manifest_str)
        
        spark.stop()

if __name__ == "__main__":
    main()
