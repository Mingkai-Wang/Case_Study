import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st

class DataVisualization:
    """
    Data visualization utilities for Menarini Data Pipeline
    """
    
    def __init__(self, pipeline=None):
        self.pipeline = pipeline
        self.color_palette = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
        ]
    
    def create_market_distribution_chart(self, data, market_col='市场类型'):
        """Create market distribution pie chart"""
        if market_col not in data.columns:
            return None
        
        market_dist = data[market_col].value_counts()
        
        fig = px.pie(
            values=market_dist.values,
            names=market_dist.index,
            title="市场类型分布",
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(
            font=dict(family="SimHei, Arial Unicode MS, sans-serif"),
            title_font_size=16
        )
        
        return fig
    
    def create_sales_trend_chart(self, data, date_col, qty_col, market_col='市场类型'):
        """Create sales trend line chart"""
        if not all(col in data.columns for col in [date_col, qty_col, market_col]):
            return None
        
        # Filter valid dates
        valid_data = data[data[date_col].notna()].copy()
        
        if valid_data.empty:
            return None
        
        # Group by month and market type
        valid_data['年月'] = valid_data[date_col].dt.to_period('M')
        trend_data = valid_data.groupby(['年月', market_col])[qty_col].sum().reset_index()
        trend_data['年月'] = trend_data['年月'].astype(str)
        
        fig = px.line(
            trend_data,
            x='年月',
            y=qty_col,
            color=market_col,
            title="销售趋势分析",
            markers=True,
            color_discrete_sequence=self.color_palette
        )
        
        fig.update_layout(
            font=dict(family="SimHei, Arial Unicode MS, sans-serif"),
            title_font_size=16,
            xaxis_title="时间",
            yaxis_title="销售数量"
        )
        
        return fig
    
    def create_market_performance_bar(self, data, market_col='市场类型', qty_col=None):
        """Create market performance bar chart"""
        if market_col not in data.columns:
            return None
        
        # Find quantity column if not specified
        if qty_col is None:
            qty_cols = [col for col in data.columns if 'QTY' in col or '数量' in col]
            if qty_cols:
                qty_col = qty_cols[0]
            else:
                return None
        
        market_performance = data.groupby(market_col)[qty_col].sum().sort_values(ascending=True)
        
        fig = px.bar(
            x=market_performance.values,
            y=market_performance.index,
            orientation='h',
            title="各市场销售表现",
            color=market_performance.values,
            color_continuous_scale='viridis'
        )
        
        fig.update_layout(
            font=dict(family="SimHei, Arial Unicode MS, sans-serif"),
            title_font_size=16,
            xaxis_title="销售数量",
            yaxis_title="市场类型",
            showlegend=False
        )
        
        return fig
    
    def create_quality_metrics_gauge(self, quality_metrics):
        """Create quality metrics gauge charts"""
        if not quality_metrics:
            return None
        
        # Calculate overall scores
        completeness_scores = [metrics.get('completeness', 0) for metrics in quality_metrics.values() if isinstance(metrics, dict)]
        validity_scores = [metrics.get('validity', 0) for metrics in quality_metrics.values() if isinstance(metrics, dict)]
        consistency_scores = [metrics.get('consistency', 0) for metrics in quality_metrics.values() if isinstance(metrics, dict)]
        
        avg_completeness = np.mean(completeness_scores) if completeness_scores else 0
        avg_validity = np.mean(validity_scores) if validity_scores else 0
        avg_consistency = np.mean(consistency_scores) if consistency_scores else 0
        
        fig = go.Figure()
        
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=avg_completeness * 100,
            domain={'x': [0, 0.3], 'y': [0, 1]},
            title={'text': "完整性"},
            delta={'reference': 80},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 60], 'color': "lightgray"},
                    {'range': [60, 80], 'color': "yellow"},
                    {'range': [80, 100], 'color': "green"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=avg_validity * 100,
            domain={'x': [0.35, 0.65], 'y': [0, 1]},
            title={'text': "有效性"},
            delta={'reference': 90},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkgreen"},
                'steps': [
                    {'range': [0, 70], 'color': "lightgray"},
                    {'range': [70, 90], 'color': "yellow"},
                    {'range': [90, 100], 'color': "green"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 95
                }
            }
        ))
        
        fig.add_trace(go.Indicator(
            mode="gauge+number+delta",
            value=avg_consistency * 100,
            domain={'x': [0.7, 1], 'y': [0, 1]},
            title={'text': "一致性"},
            delta={'reference': 95},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkorange"},
                'steps': [
                    {'range': [0, 80], 'color': "lightgray"},
                    {'range': [80, 95], 'color': "yellow"},
                    {'range': [95, 100], 'color': "green"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 98
                }
            }
        ))
        
        fig.update_layout(
            title="数据质量综合评估",
            font=dict(family="SimHei, Arial Unicode MS, sans-serif"),
            height=300
        )
        
        return fig
    
    def create_data_volume_timeline(self, pipeline):
        """Create data volume timeline chart"""
        if not pipeline or 'bronze' not in pipeline.data_lake:
            return None
        
        volume_data = []
        
        for sheet_name, sheet_info in pipeline.data_lake['bronze'].items():
            if '说明' not in sheet_name:
                volume_data.append({
                    'Sheet': sheet_name,
                    'Records': sheet_info['original_shape'][0],
                    'Columns': sheet_info['original_shape'][1],
                    'Extraction_Time': sheet_info['extracted_at']
                })
        
        if not volume_data:
            return None
        
        df_volume = pd.DataFrame(volume_data)
        
        fig = px.bar(
            df_volume,
            x='Sheet',
            y='Records',
            title="各数据源记录数量",
            color='Records',
            color_continuous_scale='blues'
        )
        
        fig.update_layout(
            font=dict(family="SimHei, Arial Unicode MS, sans-serif"),
            title_font_size=16,
            xaxis_title="数据源",
            yaxis_title="记录数量",
            xaxis_tickangle=-45
        )
        
        return fig
    
    def create_processing_summary_table(self, pipeline):
        """Create processing summary table"""
        if not pipeline:
            return None
        
        summary_data = []
        
        # Bronze layer summary
        for sheet_name, sheet_info in pipeline.data_lake.get('bronze', {}).items():
            summary_data.append({
                '层级': 'Bronze (原始)',
                '数据源': sheet_name,
                '记录数': sheet_info['original_shape'][0],
                '列数': sheet_info['original_shape'][1],
                '处理时间': sheet_info['extracted_at'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Silver layer summary
        for sheet_name, sheet_info in pipeline.data_lake.get('silver', {}).items():
            summary_data.append({
                '层级': 'Silver (清洗)',
                '数据源': sheet_name,
                '记录数': sheet_info['final_shape'][0],
                '列数': sheet_info['final_shape'][1],
                '处理时间': sheet_info['processed_at'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # Gold layer summary
        if 'unified_model' in pipeline.data_lake.get('gold', {}):
            unified_info = pipeline.data_lake['gold']['unified_model']
            summary_data.append({
                '层级': 'Gold (业务)',
                '数据源': 'Unified Model',
                '记录数': len(unified_info['data']),
                '列数': len(unified_info['data'].columns),
                '处理时间': unified_info['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        return pd.DataFrame(summary_data) if summary_data else None
