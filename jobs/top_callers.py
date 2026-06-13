import argparse
import json
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

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


def main():
    parser = argparse.ArgumentParser(description="Top 100 callers by spend")
    parser.add_argument("--input", default="/data/cdr_data.csv", help="Input CDR file path")
    parser.add_argument("--output-dir", default="/output/top_callers_by_spend", help="Base output directory")
    parser.add_argument("--run-id", required=True, help="Execution run ID")
    args = parser.parse_args()

    job_name = "top_callers_by_spend"
    run_id = args.run_id
    
    # Paths
    hdfs_base_out = f"hdfs://namenode:9000/output/top_callers_by_spend/{run_id}"
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
        
        # Aggregate total spend per caller_id
        # Schema columns: caller_id, charge_amount
        aggregated = df.groupBy("caller_id") \
            .agg(spark_sum("charge_amount").alias("total_spend")) \
            .orderBy(col("total_spend").desc()) \
            .limit(100)
            
        output_count = aggregated.count()
        
        # Write to HDFS
        # We coalesce to 1 to write as a single CSV file inside the directory
        aggregated.coalesce(1).write.mode("overwrite").csv(hdfs_base_out, header=False)
        
        # Write to local filesystem (shared volume) using python standard library to bypass mount rename issues
        os.makedirs(local_base_out, exist_ok=True)
        local_csv_path = os.path.join(local_base_out, "part-00000.csv")
        local_data = aggregated.collect()
        with open(local_csv_path, "w", encoding="utf-8") as f:
            for row in local_data:
                f.write(f"{row['caller_id']},{row['total_spend']}\n")
        
        print(f"Job completed successfully. Input count: {input_count}, Output count: {output_count}")
        
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
            "output_path": f"/output/top_callers_by_spend/{run_id}/",
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
