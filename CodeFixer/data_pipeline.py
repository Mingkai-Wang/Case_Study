import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
import json
import sqlite3
from pathlib import Path
import warnings
import io
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MenariniDataPipeline:
    """
    Complete Data Pipeline Solution for Menarini Asia Pacific
    
    This class implements:
    1. Automated Data Ingestion
    2. Data Transformation and Enrichment
    3. Data Modeling and Storage
    4. Data Governance and Documentation
    """
    
    def __init__(self, config_path=None):
        self.config = self._load_config(config_path)
        self.raw_data = {}
        self.processed_data = {}
        self.data_quality_report = {}
        self.lineage_tracker = {}
        
        # Initialize data lake structure
        self.data_lake = {
            'bronze': {},  # Raw data
            'silver': {},  # Cleaned data
            'gold': {}     # Business-ready data
        }
        
        # Set up Chinese font support
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
    def _load_config(self, config_path):
        """Load pipeline configuration"""
        default_config = {
            'data_sources': {
                'sales_data': 'Case Study - Data & AI Engineer.xlsx',
                'market_segments': ['RX', '电子商务', 'Device', 'Retail', 'CSO&DSO', '非目标市场'],
                'key_metrics': ['QTY数量', 'OrderDate订单日期', 'ItemName产品名称']
            },
            'quality_thresholds': {
                'completeness': 0.8,
                'validity': 0.9,
                'consistency': 0.95
            },
            'business_rules': {
                'min_order_qty': 1,
                'valid_date_range': ['2020-01-01', '2025-12-31'],
                'required_fields': ['ID', 'ItemName产品名称', 'QTY数量']
            }
        }
        return default_config
    
    # ==========================================================================
    # 1. AUTOMATED DATA INGESTION
    # ==========================================================================
    
    def extract_data_from_bytes(self, file_bytes, filename):
        """
        Extract data from uploaded file bytes with error handling
        """
        logger.info(f"Starting data extraction from uploaded file: {filename}")
        
        try:
            # Create file-like object from bytes
            file_obj = io.BytesIO(file_bytes)
            
            # Load all sheets
            all_sheets = pd.read_excel(file_obj, sheet_name=None)
            
            for sheet_name, df in all_sheets.items():
                logger.info(f"Extracted sheet: {sheet_name} with shape {df.shape}")
                
                # Store in bronze layer (raw data)
                self.data_lake['bronze'][sheet_name] = {
                    'data': df,
                    'extracted_at': datetime.now(),
                    'source': filename,
                    'original_shape': df.shape
                }
                
                # Track data lineage
                self.lineage_tracker[sheet_name] = {
                    'source': filename,
                    'extraction_timestamp': datetime.now(),
                    'transformations': []
                }
            
            logger.info(f"Successfully extracted {len(all_sheets)} sheets")
            return True
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            return False
    
    def extract_data_from_excel(self, file_path):
        """
        Extract data from Excel with error handling and retry mechanisms
        """
        logger.info(f"Starting data extraction from {file_path}")
        
        try:
            # Load all sheets
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            
            for sheet_name, df in all_sheets.items():
                logger.info(f"Extracted sheet: {sheet_name} with shape {df.shape}")
                
                # Store in bronze layer (raw data)
                self.data_lake['bronze'][sheet_name] = {
                    'data': df,
                    'extracted_at': datetime.now(),
                    'source': file_path,
                    'original_shape': df.shape
                }
                
                # Track data lineage
                self.lineage_tracker[sheet_name] = {
                    'source': file_path,
                    'extraction_timestamp': datetime.now(),
                    'transformations': []
                }
            
            logger.info(f"Successfully extracted {len(all_sheets)} sheets")
            return True
            
        except Exception as e:
            logger.error(f"Data extraction failed: {e}")
            return False
    
    def validate_data_sources(self):
        """Validate extracted data sources"""
        validation_results = {}
        
        for sheet_name, sheet_info in self.data_lake['bronze'].items():
            df = sheet_info['data']
            
            validation_results[sheet_name] = {
                'is_empty': df.empty,
                'has_duplicates': df.duplicated().any(),
                'missing_data_percentage': (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100,
                'data_types': dict(df.dtypes)
            }
        
        logger.info("Data source validation completed")
        return validation_results
    
    # ==========================================================================
    # 2. DATA TRANSFORMATION AND ENRICHMENT
    # ==========================================================================
    
    def clean_column_names(self, df, sheet_name):
        """Standardize column names"""
        original_columns = df.columns.tolist()
        
        # Clean column names
        df.columns = (df.columns
                     .str.strip()
                     .str.replace(r'[\n\r]', '', regex=True)
                     .str.replace(r'\s+', '', regex=True))
        
        # Track transformation
        self.lineage_tracker[sheet_name]['transformations'].append({
            'step': 'column_name_cleaning',
            'timestamp': datetime.now(),
            'changes': {
                'before': original_columns,
                'after': df.columns.tolist()
            }
        })
        
        return df
    
    def standardize_data_types(self, df, sheet_name):
        """Standardize data types across all sheets"""
        
        # Quantity columns
        qty_cols = [col for col in df.columns if any(k in col for k in ['QTY', '数量'])]
        for col in qty_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Date columns - improved handling
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate', 'reportmonth'])]
        for col in date_cols:
            # Try multiple date parsing strategies
            if not df[col].empty:
                # Strategy 1: Standard pandas conversion
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if df[col].notna().sum() > 0:
                        continue
                except:
                    pass
                
                # Strategy 2: Handle Excel date serial numbers
                try:
                    numeric_dates = pd.to_numeric(df[col], errors='coerce')
                    if numeric_dates.notna().sum() > 0:
                        # Excel date serial number starts from 1900-01-01
                        df[col] = pd.to_datetime('1899-12-30') + pd.to_timedelta(numeric_dates, 'D')
                        # Validate reasonable date range
                        mask = (df[col].dt.year >= 2020) & (df[col].dt.year <= 2025)
                        df.loc[~mask, col] = pd.NaT
                except:
                    pass
        
        # Text columns
        text_cols = df.select_dtypes(include=['object']).columns
        for col in text_cols:
            if col not in date_cols:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', ''], np.nan)
        
        return df
    
    def apply_business_rules(self, df, sheet_name):
        """Apply business validation rules"""
        rules_applied = []
        
        # Rule 1: Minimum order quantity
        qty_cols = [col for col in df.columns if any(k in col for k in ['QTY', '数量'])]
        if qty_cols:
            qty_col = qty_cols[0]
            invalid_qty_mask = df[qty_col] < self.config['business_rules']['min_order_qty']
            invalid_count = invalid_qty_mask.sum()
            if invalid_count > 0:
                df.loc[invalid_qty_mask, qty_col] = np.nan
                rules_applied.append(f"Removed {invalid_count} records with invalid quantity")
        
        # Rule 2: Date range validation
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate'])]
        for col in date_cols:
            if df[col].dtype == 'datetime64[ns]':
                start_date = pd.to_datetime(self.config['business_rules']['valid_date_range'][0])
                end_date = pd.to_datetime(self.config['business_rules']['valid_date_range'][1])
                
                invalid_date_mask = (df[col] < start_date) | (df[col] > end_date)
                invalid_dates = invalid_date_mask.sum()
                if invalid_dates > 0:
                    df.loc[invalid_date_mask, col] = pd.NaT
                    rules_applied.append(f"Removed {invalid_dates} records with invalid dates in {col}")
        
        # Track rules applied
        self.lineage_tracker[sheet_name]['transformations'].append({
            'step': 'business_rules_application',
            'timestamp': datetime.now(),
            'rules_applied': rules_applied
        })
        
        return df
    
    def enrich_data(self, df, sheet_name):
        """Enrich data with additional business context"""
        
        # Add market type based on sheet name
        if 'RX' in sheet_name:
            df['市场类型'] = 'RX处方药市场'
        elif '电子商务' in sheet_name:
            df['市场类型'] = '电子商务市场'
        elif 'Device' in sheet_name:
            df['市场类型'] = '医疗器械市场'
        elif 'Retail' in sheet_name:
            df['市场类型'] = '零售市场'
        elif 'CSO' in sheet_name or 'DSO' in sheet_name:
            df['市场类型'] = 'CSO&DSO市场'
        elif '非目标' in sheet_name:
            df['市场类型'] = '非目标市场'
        else:
            df['市场类型'] = '其他市场'
        
        # Add time-based features
        date_cols = [col for col in df.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate'])]
        if date_cols and df[date_cols[0]].dtype == 'datetime64[ns]':
            date_col = date_cols[0]
            df['年份'] = df[date_col].dt.year
            df['月份'] = df[date_col].dt.month
            df['季度'] = df[date_col].dt.quarter
            df['星期几'] = df[date_col].dt.dayofweek
        
        # Add data processing metadata
        df['数据处理时间'] = datetime.now()
        df['数据来源'] = sheet_name
        
        return df
    
    def comprehensive_etl_process(self):
        """Execute comprehensive ETL process"""
        logger.info("Starting comprehensive ETL process")
        
        for sheet_name, sheet_info in self.data_lake['bronze'].items():
            if '说明' in sheet_name:  # Skip explanation sheets
                continue
                
            logger.info(f"Processing sheet: {sheet_name}")
            
            # Extract from bronze layer
            df = sheet_info['data'].copy()
            
            # Transform
            df = self.clean_column_names(df, sheet_name)
            df = df.dropna(axis=1, how='all')  # Remove empty columns
            df = df.dropna(axis=0, how='all')  # Remove empty rows
            df = self.standardize_data_types(df, sheet_name)
            df = self.apply_business_rules(df, sheet_name)
            df = self.enrich_data(df, sheet_name)
            
            # Store in silver layer (cleaned data)
            self.data_lake['silver'][sheet_name] = {
                'data': df,
                'processed_at': datetime.now(),
                'transformations_applied': len(self.lineage_tracker[sheet_name]['transformations']),
                'final_shape': df.shape
            }
            
            logger.info(f"Completed processing {sheet_name}: {df.shape}")
    
    # ==========================================================================
    # 3. DATA MODELING AND STORAGE
    # ==========================================================================
    
    def create_unified_data_model(self):
        """Create unified data model for business intelligence"""
        logger.info("Creating unified data model")
        
        # Combine all market segments into unified model
        unified_data = []
        common_columns = None
        
        for sheet_name, sheet_info in self.data_lake['silver'].items():
            if '说明' in sheet_name or '产品' in sheet_name:
                continue
                
            df = sheet_info['data']
            
            # Find common columns across all sheets
            if common_columns is None:
                common_columns = set(df.columns)
            else:
                common_columns = common_columns.intersection(set(df.columns))
        
        # Create unified dataset with common columns
        if common_columns:
            common_columns = list(common_columns)
            
            for sheet_name, sheet_info in self.data_lake['silver'].items():
                if '说明' in sheet_name or '产品' in sheet_name:
                    continue
                    
                df = sheet_info['data']
                
                # Select common columns
                df_common = df[common_columns].copy()
                unified_data.append(df_common)
            
            if unified_data:
                # Combine all data
                unified_df = pd.concat(unified_data, ignore_index=True)
                
                # Store in gold layer
                self.data_lake['gold']['unified_model'] = {
                    'data': unified_df,
                    'created_at': datetime.now(),
                    'source_sheets': [name for name in self.data_lake['silver'].keys() if '说明' not in name and '产品' not in name],
                    'common_columns': common_columns
                }
                
                logger.info(f"Created unified data model with {len(unified_df)} records and {len(common_columns)} columns")
        
    # ==========================================================================
    # 4. DATA QUALITY ASSESSMENT
    # ==========================================================================
    
    def assess_data_quality(self):
        """Comprehensive data quality assessment"""
        logger.info("Starting data quality assessment")
        
        for sheet_name, sheet_info in self.data_lake['silver'].items():
            df = sheet_info['data']
            
            # Calculate completeness
            total_cells = df.shape[0] * df.shape[1]
            non_null_cells = df.notna().sum().sum()
            completeness = non_null_cells / total_cells if total_cells > 0 else 0
            
            # Calculate validity (for numeric and date columns)
            validity_scores = []
            
            # Numeric validity
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if not df[col].empty:
                    valid_count = df[col].notna().sum()
                    total_count = len(df[col])
                    if total_count > 0:
                        validity_scores.append(valid_count / total_count)
            
            # Date validity
            date_cols = df.select_dtypes(include=['datetime64']).columns
            for col in date_cols:
                if not df[col].empty:
                    valid_count = df[col].notna().sum()
                    total_count = len(df[col])
                    if total_count > 0:
                        validity_scores.append(valid_count / total_count)
            
            validity = np.mean(validity_scores) if validity_scores else 1.0
            
            # Calculate consistency (duplicate check)
            duplicate_rate = df.duplicated().sum() / len(df) if len(df) > 0 else 0
            consistency = 1 - duplicate_rate
            
            # Store quality metrics
            self.data_quality_report[sheet_name] = {
                'completeness': completeness,
                'validity': validity,
                'consistency': consistency,
                'total_records': len(df),
                'total_columns': len(df.columns),
                'assessment_time': datetime.now()
            }
        
        logger.info("Data quality assessment completed")
    
    def generate_data_dictionary(self):
        """Generate comprehensive data dictionary"""
        data_dictionary = {}
        
        for layer_name, layer_data in self.data_lake.items():
            data_dictionary[layer_name] = {}
            
            for dataset_name, dataset_info in layer_data.items():
                if 'data' in dataset_info:
                    df = dataset_info['data']
                    
                    columns_info = {}
                    for col in df.columns:
                        columns_info[col] = {
                            'data_type': str(df[col].dtype),
                            'non_null_count': df[col].notna().sum(),
                            'null_count': df[col].isna().sum(),
                            'unique_values': df[col].nunique(),
                            'sample_values': df[col].dropna().head(3).tolist() if not df[col].dropna().empty else []
                        }
                    
                    data_dictionary[layer_name][dataset_name] = {
                        'columns': columns_info,
                        'total_rows': len(df),
                        'total_columns': len(df.columns),
                        'creation_time': dataset_info.get('processed_at', dataset_info.get('extracted_at', 'Unknown'))
                    }
        
        return data_dictionary
