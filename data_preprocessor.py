# data_preprocessor.py
import pandas as pd
import numpy as np

def preprocess_data(file_path):
    """
    Preprocess data using the approach from the Jupyter notebook.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        DataFrame: Cleaned and preprocessed DataFrame
    """
    try:
        # Load original dataset
        df_original = pd.read_csv(file_path)
        df = df_original.copy()
        
        print(f"Original dataset shape: {df.shape}")
        
        # Step 1: Drop columns with no variance
        dup_cols = [col for col in df.columns if df[col].nunique(dropna=False) == 1]
        df = df.drop(columns=dup_cols)
        print(f"Dropped {len(dup_cols)} columns with no variance")
        print(f"Columns dropped due to no variance: {dup_cols}")
        
        # Step 2: Drop highly correlated columns
        columns_to_remove = ['RecordNumber', 'event_id', 'id']
        columns_to_remove = [col for col in columns_to_remove if col in df.columns]
        if columns_to_remove:
            df = df.drop(columns=columns_to_remove)
            print(f"Dropped {len(columns_to_remove)} highly correlated columns")
            print(f"Columns dropped due to high correlation: {columns_to_remove}")
        
        # Step 3: Check for missing values
        missing_values = df.isnull().sum()
        print("Missing values in remaining columns:")
        print(missing_values[missing_values > 0])
        
        # Step 4: Check for duplicates
        duplicates = df.duplicated().sum()
        print(f"Number of duplicate rows: {duplicates}")
        
        print(f"Final preprocessed dataset shape: {df.shape}")
        return df
        
    except Exception as e:
        print(f"Error preprocessing data: {e}")
        return None

def analyze_correlation(df, threshold=0.95):
    """
    Analyze correlation between numeric columns.
    
    Args:
        df (DataFrame): DataFrame to analyze
        threshold (float): Correlation threshold to report
        
    Returns:
        dict: Dictionary with correlation information
    """
    try:
        # Get only numeric columns
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.shape[1] < 2:
            return {"error": "Not enough numeric columns for correlation analysis"}
            
        # Calculate correlation matrix
        corr_matrix = numeric_df.corr()
        
        # Find highly correlated pairs
        high_corr_pairs = []
        
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if abs(corr_matrix.iloc[i, j]) >= threshold:
                    high_corr_pairs.append({
                        "column1": corr_matrix.columns[i],
                        "column2": corr_matrix.columns[j],
                        "correlation": corr_matrix.iloc[i, j]
                    })
        
        return {
            "numeric_columns": numeric_df.columns.tolist(),
            "high_correlation_pairs": high_corr_pairs
        }
        
    except Exception as e:
        return {"error": str(e)}