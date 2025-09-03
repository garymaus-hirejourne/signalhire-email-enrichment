#!/usr/bin/env python3
"""
CSV File Combiner for SignalHire Banking Data
Combines multiple CSV files with same header structure into one file.
Keeps header from first file, removes headers from subsequent files.
"""

import pandas as pd
import os
from pathlib import Path
import glob

def normalize_column_name(col_name):
    """Normalize column names for matching"""
    if pd.isna(col_name) or col_name == '':
        return 'Unknown'
    return str(col_name).strip().lower().replace(' ', '').replace('_', '').replace('-', '')

def match_columns_by_name(source_columns, master_columns):
    """Match columns by naming convention similarity"""
    normalized_master = {normalize_column_name(col): col for col in master_columns}
    column_mapping = {}
    
    for src_col in source_columns:
        normalized_src = normalize_column_name(src_col)
        if normalized_src in normalized_master:
            column_mapping[src_col] = normalized_master[normalized_src]
        else:
            # Find closest match
            best_match = None
            best_score = 0
            for norm_master, master_col in normalized_master.items():
                # Simple substring matching
                if normalized_src in norm_master or norm_master in normalized_src:
                    score = min(len(normalized_src), len(norm_master)) / max(len(normalized_src), len(norm_master))
                    if score > best_score:
                        best_score = score
                        best_match = master_col
            
            if best_match and best_score > 0.5:
                column_mapping[src_col] = best_match
            else:
                column_mapping[src_col] = None  # No match found
    
    return column_mapping

def combine_csv_files(source_directory, output_filename="NEW.csv"):
    """
    Combine all CSV files in a directory into one file with intelligent column matching.
    
    Args:
        source_directory (str): Path to directory containing CSV files
        output_filename (str): Name of output file (default: NEW.csv)
    """
    
    source_path = Path(source_directory)
    
    # Find all CSV files in the directory, excluding the output file
    csv_files = [f for f in source_path.glob("*.csv") if f.name != output_filename]
    
    if not csv_files:
        print(f"No CSV files found in {source_directory}")
        return
    
    # Sort files to ensure consistent order (SignalHire_exports (1), (2), etc.)
    csv_files.sort(key=lambda x: x.name)
    
    print(f"Found {len(csv_files)} CSV files to combine:")
    for file in csv_files:
        print(f"  - {file.name}")
    
    combined_data = []
    master_columns = None
    
    # Process all files with intelligent column matching
    for i, csv_file in enumerate(csv_files):
        print(f"\nProcessing {csv_file.name}...")
        try:
            # Try UTF-8 encoding first, then fallback to other encodings
            try:
                df = pd.read_csv(csv_file, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(csv_file, encoding='latin-1')
                except UnicodeDecodeError:
                    df = pd.read_csv(csv_file, encoding='cp1252')
            
            if i == 0:
                # First file establishes the master structure
                master_columns = df.columns.tolist()
                combined_data.append(df)
                print(f"  Master file: Added {len(df)} rows with {len(df.columns)} columns")
                print(f"  Master columns: {', '.join(master_columns)}")
            else:
                # Match columns by naming convention
                current_columns = df.columns.tolist()
                print(f"  Current columns: {', '.join(current_columns)}")
                
                column_mapping = match_columns_by_name(current_columns, master_columns)
                
                # Create new dataframe with master column structure
                new_df = pd.DataFrame(columns=master_columns)
                
                # Map data from current file to master structure
                for src_col, target_col in column_mapping.items():
                    if target_col is not None:
                        new_df[target_col] = df[src_col]
                        try:
                            print(f"    Mapped: {src_col} -> {target_col}")
                        except UnicodeEncodeError:
                            print(f"    Mapped: [column] -> [column]")
                    else:
                        try:
                            print(f"    Unmapped: {src_col} (no suitable match)")
                        except UnicodeEncodeError:
                            print(f"    Unmapped: [column] (no suitable match)")
                
                # Fill any remaining columns with empty values
                for col in master_columns:
                    if col not in new_df.columns or new_df[col].isna().all():
                        new_df[col] = ""
                
                combined_data.append(new_df)
                print(f"  Added {len(new_df)} rows with intelligent column mapping")
                
        except Exception as e:
            print(f"  Error reading {csv_file.name}: {e}")
            print(f"  Skipping this file...")
            continue
    
    # Combine all dataframes
    print("\nCombining all data...")
    final_df = pd.concat(combined_data, ignore_index=True)
    
    # Save to output file with proper encoding
    output_path = source_path / output_filename
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"\nSUCCESS!")
    print(f"Combined {len(csv_files)} files into: {output_path}")
    print(f"Total rows: {len(final_df)} (including header)")
    print(f"Total columns: {len(final_df.columns)}")
    
    # Show column names
    print(f"\nColumns: {', '.join(final_df.columns.tolist())}")
    
    return output_path

if __name__ == "__main__":
    # Directory containing the CSV files
    target_directory = r"G:\My Drive\Hirejoure.com\Dyer, Bryan\Top 100 For-Profit Universities\RAW Data\11-92"
    
    print("CSV File Combiner")
    print("=" * 50)
    
    # Combine the files
    result = combine_csv_files(target_directory, "NEW.csv")
    
    if result:
        print(f"\nCombined file created successfully: {result}")
    else:
        print("\nFailed to create combined file")
