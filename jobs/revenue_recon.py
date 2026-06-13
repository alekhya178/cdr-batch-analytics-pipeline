import argparse
import json
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession  # pyrefly: ignore [missing-import]
from pyspark.sql.functions import sum as spark_sum  # pyrefly: ignore [missing-import]

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
    parser = argparse.ArgumentParser(description="Revenue reconciliation job")
    parser.add_argument("--input", default="/data/cdr_data.csv", help="Input CDR file path")
    parser.add_argument("--output-dir", default="/output/revenue_reconciliation", help="Base output directory")
    parser.add_argument("--run-id", required=True, help="Execution run ID")
    args = parser.parse_args()

    job_name = "revenue_reconciliation"
    run_id = args.run_id
    
    # Paths
    hdfs_base_out = f"hdfs://namenode:9000/output/revenue_reconciliation/{run_id}"
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
        
        # Calculate total revenue sum
        total_val_row = df.select(spark_sum("charge_amount")).collect()
        total_revenue = 0.0
        if total_val_row and total_val_row[0][0] is not None:
            total_revenue = float(total_val_row[0][0])
            
        # Create single row DataFrame
        recon_df = spark.createDataFrame([(total_revenue,)], ["total_revenue"])
        output_count = 1
        
        # Write to HDFS
        recon_df.coalesce(1).write.mode("overwrite").csv(hdfs_base_out, header=False)
        
        # Write to local filesystem (shared volume) using python standard library to bypass mount rename issues
        os.makedirs(local_base_out, exist_ok=True)
        local_csv_path = os.path.join(local_base_out, "part-00000.csv")
        with open(local_csv_path, "w", encoding="utf-8") as f:
            f.write(f"{total_revenue}\n")
        
        print(f"Job completed successfully. Input count: {input_count}, Revenue reconciliation: {total_revenue}")
        
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
            "output_path": f"/output/revenue_reconciliation/{run_id}/",
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
