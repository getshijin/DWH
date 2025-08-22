import os
import pandas as pd

def split_csv(input_file, output_dir, chunk_size_mb=250, encoding="utf-8"):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Calculate rows per chunk roughly based on file size
    file_size = os.path.getsize(input_file)
    with open(input_file, encoding=encoding, errors="ignore") as f:
        total_rows = sum(1 for _ in f) - 1  # exclude header
    
    rows_per_chunk = int((chunk_size_mb * 1024 * 1024 / file_size) * total_rows)

    print(f"Splitting '{input_file}' ({file_size/1024/1024:.2f} MB, {total_rows} rows)...")
    print(f"Approx {rows_per_chunk} rows per chunk...")

    # Read and split in chunks
    part_num = 0
    for chunk in pd.read_csv(input_file, chunksize=rows_per_chunk, encoding=encoding, low_memory=False):
        part_filename = os.path.join(
            output_dir,
            f"{os.path.splitext(os.path.basename(input_file))[0]}_part{part_num:03d}.csv"
        )
        chunk.to_csv(part_filename, index=False, encoding=encoding)
        print(f"Created: {part_filename} ({os.path.getsize(part_filename)/1024/1024:.2f} MB)")
        part_num += 1

    print("✅ Splitting complete!")

# Example usage:
split_csv(
    r"E:\Datasets\lendingclub\accepted_2007_to_2018Q4.csv",
    r"E:\Datasets\lendingclub\split_parts",  # this folder will now be created automatically
    250,
    encoding="utf-8"
)
