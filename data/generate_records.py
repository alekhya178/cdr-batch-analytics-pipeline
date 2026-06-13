import csv
import random
from datetime import datetime, timedelta

def generate_data():
    output_path = "/data/cdr_data.csv"
    total_records = 2050000
    whale_records_count = 210000 # > 10% of total
    
    # Seed for reproducibility
    random.seed(42)
    
    print("Generating Call Detail Records (CDRs)...")
    
    callers = [f"caller_{i}" for i in range(1, 10000)]
    receivers = [f"receiver_{i}" for i in range(1, 10000)]
    towers = [f"tower_{i}" for i in range(1, 101)]
    call_types = ["VOICE", "SMS", "DATA"]
    
    start_time = datetime(2026, 6, 1, 0, 0, 0)
    
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["caller_id", "receiver_id", "duration_sec", "tower_id", "timestamp", "call_type", "charge_amount"])
        
        # 1. Generate Whale Caller records
        whale_caller = "caller_whale"
        print(f"Generating {whale_records_count} records for whale caller: {whale_caller}")
        # Write whale caller in batches
        batch = []
        for _ in range(whale_records_count):
            delta_sec = random.randint(0, 30 * 24 * 3600)
            ts = start_time + timedelta(seconds=delta_sec)
            ts_str = ts.isoformat() + "Z"
            
            call_type = random.choice(call_types)
            if call_type == "SMS":
                duration = 0
                charge = round(random.uniform(0.01, 0.10), 2)
            elif call_type == "DATA":
                duration = random.randint(1, 600)
                charge = round(random.uniform(0.05, 5.00), 2)
            else:
                duration = random.randint(5, 3000)
                charge = round(duration * 0.01, 2)
                
            batch.append([
                whale_caller,
                random.choice(receivers),
                duration,
                random.choice(towers),
                ts_str,
                call_type,
                charge
            ])
            if len(batch) >= 100000:
                writer.writerows(batch)
                batch = []
        if batch:
            writer.writerows(batch)
            batch = []
            
        # 2. Generate Anomalous Callers
        # 100 callers with 20 regular 60s calls and 1 anomalous 3600s call
        print("Generating anomalous callers...")
        for j in range(1, 100):
            caller_id = f"caller_anomaly_{j}"
            # 20 regular calls of 60 seconds
            for i in range(20):
                ts = start_time + timedelta(minutes=j*30 + i*5)
                ts_str = ts.isoformat() + "Z"
                batch.append([
                    caller_id,
                    random.choice(receivers),
                    60,
                    random.choice(towers),
                    ts_str,
                    "VOICE",
                    0.60
                ])
            # 1 anomalous call of 3600 seconds
            ts = start_time + timedelta(minutes=j*30 + 105)
            ts_str = ts.isoformat() + "Z"
            batch.append([
                caller_id,
                random.choice(receivers),
                3600,
                random.choice(towers),
                ts_str,
                "VOICE",
                36.00
            ])
            if len(batch) >= 100000:
                writer.writerows(batch)
                batch = []
        if batch:
            writer.writerows(batch)
            batch = []
            
        # 3. Generate remaining random records to reach total_records
        remaining_count = total_records - whale_records_count - (99 * 21)
        print(f"Generating {remaining_count} random records...")
        
        for _ in range(remaining_count):
            caller = random.choice(callers)
            receiver = random.choice(receivers)
            call_type = random.choice(call_types)
            
            if call_type == "SMS":
                duration = 0
                charge = round(random.uniform(0.01, 0.10), 2)
            elif call_type == "DATA":
                duration = random.randint(1, 1800)
                charge = round(random.uniform(0.05, 10.00), 2)
            else: # VOICE
                duration = random.randint(5, 3600)
                charge = round(duration * 0.01, 2)
                
            delta_sec = random.randint(0, 30 * 24 * 3600)
            ts = start_time + timedelta(seconds=delta_sec)
            ts_str = ts.isoformat() + "Z"
            
            batch.append([
                caller,
                receiver,
                duration,
                random.choice(towers),
                ts_str,
                call_type,
                charge
            ])
            
            if len(batch) >= 100000:
                writer.writerows(batch)
                batch = []
                
        if batch:
            writer.writerows(batch)
            
    print("CDR records generated successfully!")

if __name__ == "__main__":
    generate_data()
