import pandas as pd
import os
import glob
from pathlib import Path

def convert_csv_to_parquet(directory="."):
    csv_files = g_files = glob.glob(os.path.join(directory, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the directory.")
        return

    print(f"Found {len(csv_files)} CSV files to convert.")
    
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']

    for csv_file in csv_files:
        success = False
        target_path = Path(csv_file).with_suffix(".parquet")
        
        print(f"\x1b[34mConverting {os.path.basename(csv_file)}...\x1b[0m")
        
        # Try multiple encodings
        for encoding in encodings:
            try:
                # Read CSV
                df = pd.read_csv(csv_file, low_memory=False, encoding=encoding)
                
                # Convert to Parquet
                df.to_parquet(target_path, engine='pyarrow', index=False)
                
                # Verification of the result
                if os.path.exists(target_path):
                    size_csv = os.path.getsize(csv_file) / (1024 * 1024)
                    size_parquet = os.path.getsize(target_path) / (1024 * 1024)
                    print(f"  \x1b[32mSuccess!\x1b[0m (Encoding: {encoding})")
                    print(f"  Size: {size_csv:.2f} MB -> {size_parquet:.2f} MB")
                    success = True
                    break
            except Exception as e:
                continue
        
        if not success:
            print(f"  \x1b[31mFailed to convert {csv_file}\x1b[0m. Please check encoding or file format.")

if __name__ == "__main__":
    convert_csv_to_parquet()
