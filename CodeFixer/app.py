import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import io
import json
from data_pipeline import MenariniDataPipeline
from visualization import DataVisualization

# Configure page
st.set_page_config(
    page_title="Menarini Asia Pacific - Data Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Chinese font support
st.markdown("""
<style>
    .stApp {
        font-family: "SimHei", "Arial Unicode MS", "DejaVu Sans", sans-serif;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border: 1px solid #c3e6cb;
    }
    .error-message {
        background-color: #f8d7da;
        color: #721c24;
        padding: 0.75rem;
        border-radius: 0.25rem;
        border: 1px solid #f5c6cb;
    }
</style>
""", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if 'pipeline' not in st.session_state:
        st.session_state.pipeline = None
    if 'data_processed' not in st.session_state:
        st.session_state.data_processed = False
    if 'uploaded_file' not in st.session_state:
        st.session_state.uploaded_file = None

def display_header():
    """Display application header"""
    st.title("🏥 Menarini Asia Pacific - 数据管道系统")
    st.markdown("---")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("数据源", "Excel文件", "支持多工作表")
    with col2:
        st.metric("市场细分", "6个", "RX, 电商, 设备等")
    with col3:
        st.metric("数据治理", "自动化", "质量检查")
    with col4:
        st.metric("状态", "就绪" if st.session_state.data_processed else "等待数据", "")

def file_upload_section():
    """Handle file upload and processing"""
    st.header("📁 数据上传与处理")
    
    uploaded_file = st.file_uploader(
        "上传 Excel 文件 (Case Study - Data & AI Engineer.xlsx)",
        type=['xlsx', 'xls'],
        help="请上传包含市场数据的Excel文件"
    )
    
    if uploaded_file is not None:
        if uploaded_file != st.session_state.uploaded_file:
            st.session_state.uploaded_file = uploaded_file
            st.session_state.data_processed = False
            st.session_state.pipeline = None
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.info(f"📄 文件已上传: {uploaded_file.name}")
            st.write(f"文件大小: {uploaded_file.size / 1024 / 1024:.2f} MB")
        
        with col2:
            if st.button("🚀 开始处理数据", type="primary"):
                process_data(uploaded_file)
    
    return uploaded_file

def process_data(uploaded_file):
    """Process uploaded data through the pipeline"""
    with st.spinner("正在处理数据，请稍候..."):
        try:
            # Save uploaded file temporarily
            file_bytes = uploaded_file.read()
            
            # Initialize pipeline
            pipeline = MenariniDataPipeline()
            
            # Extract data from uploaded file
            with st.expander("📊 数据提取日志", expanded=False):
                log_container = st.empty()
                
                # Extract data
                success = pipeline.extract_data_from_bytes(file_bytes, uploaded_file.name)
                
                if success:
                    log_container.success("✅ 数据提取成功")
                    
                    # Validate data sources
                    validation_results = pipeline.validate_data_sources()
                    
                    # Perform ETL process
                    pipeline.comprehensive_etl_process()
                    
                    # Create unified data model
                    pipeline.create_unified_data_model()
                    
                    # Generate quality report
                    pipeline.assess_data_quality()
                    
                    st.session_state.pipeline = pipeline
                    st.session_state.data_processed = True
                    
                    st.success("🎉 数据处理完成！")
                    st.rerun()
                else:
                    log_container.error("❌ 数据提取失败")
                    
        except Exception as e:
            st.error(f"❌ 处理过程中出现错误: {str(e)}")

def display_data_overview():
    """Display data overview and quality metrics"""
    if not st.session_state.data_processed or not st.session_state.pipeline:
        st.warning("⚠️ 请先上传并处理数据文件")
        return
    
    st.header("📈 数据概览")
    
    pipeline = st.session_state.pipeline
    
    # Display bronze layer info
    st.subheader("🥉 原始数据层 (Bronze Layer)")
    bronze_data = pipeline.data_lake['bronze']
    
    cols = st.columns(len(bronze_data))
    for i, (sheet_name, sheet_info) in enumerate(bronze_data.items()):
        with cols[i % len(cols)]:
            st.metric(
                label=sheet_name,
                value=f"{sheet_info['original_shape'][0]} 行",
                delta=f"{sheet_info['original_shape'][1]} 列"
            )
    
    # Display silver layer info
    st.subheader("🥈 清洗数据层 (Silver Layer)")
    silver_data = pipeline.data_lake['silver']
    
    if silver_data:
        df_info = []
        for sheet_name, sheet_info in silver_data.items():
            df_info.append({
                '工作表': sheet_name,
                '行数': sheet_info['final_shape'][0],
                '列数': sheet_info['final_shape'][1],
                '转换步骤': sheet_info['transformations_applied'],
                '处理时间': sheet_info['processed_at'].strftime('%Y-%m-%d %H:%M:%S')
            })
        
        st.dataframe(pd.DataFrame(df_info), use_container_width=True)
    
    # Display gold layer info
    st.subheader("🥇 业务数据层 (Gold Layer)")
    if 'unified_model' in pipeline.data_lake['gold']:
        unified_data = pipeline.data_lake['gold']['unified_model']['data']
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("统一数据集行数", f"{len(unified_data):,}")
        with col2:
            st.metric("统一数据集列数", len(unified_data.columns))
        with col3:
            total_qty = unified_data[[col for col in unified_data.columns if 'QTY' in col or '数量' in col]].sum().sum()
            st.metric("总销售数量", f"{total_qty:,.0f}")

def display_data_quality():
    """Display data quality assessment"""
    if not st.session_state.data_processed or not st.session_state.pipeline:
        st.warning("⚠️ 请先上传并处理数据文件")
        return
    
    st.header("🔍 数据质量评估")
    
    pipeline = st.session_state.pipeline
    
    if hasattr(pipeline, 'data_quality_report') and pipeline.data_quality_report:
        # Overall quality metrics
        st.subheader("整体质量指标")
        
        total_completeness = 0
        total_validity = 0
        total_consistency = 0
        sheet_count = 0
        
        quality_data = []
        
        for sheet_name, quality_metrics in pipeline.data_quality_report.items():
            if isinstance(quality_metrics, dict):
                completeness = quality_metrics.get('completeness', 0)
                validity = quality_metrics.get('validity', 0)
                consistency = quality_metrics.get('consistency', 0)
                
                total_completeness += completeness
                total_validity += validity
                total_consistency += consistency
                sheet_count += 1
                
                quality_data.append({
                    '工作表': sheet_name,
                    '完整性': f"{completeness:.1%}",
                    '有效性': f"{validity:.1%}",
                    '一致性': f"{consistency:.1%}",
                    '整体评分': f"{(completeness + validity + consistency) / 3:.1%}"
                })
        
        if sheet_count > 0:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                avg_completeness = total_completeness / sheet_count
                st.metric("平均完整性", f"{avg_completeness:.1%}")
            
            with col2:
                avg_validity = total_validity / sheet_count
                st.metric("平均有效性", f"{avg_validity:.1%}")
            
            with col3:
                avg_consistency = total_consistency / sheet_count
                st.metric("平均一致性", f"{avg_consistency:.1%}")
            
            # Quality details table
            st.subheader("详细质量报告")
            st.dataframe(pd.DataFrame(quality_data), use_container_width=True)
    else:
        st.info("数据质量报告正在生成中...")

def display_market_analysis():
    """Display market segment analysis"""
    if not st.session_state.data_processed or not st.session_state.pipeline:
        st.warning("⚠️ 请先上传并处理数据文件")
        return
    
    st.header("🏪 市场细分分析")
    
    pipeline = st.session_state.pipeline
    
    if 'unified_model' in pipeline.data_lake['gold']:
        unified_data = pipeline.data_lake['gold']['unified_model']['data']
        
        # Market type distribution
        if '市场类型' in unified_data.columns:
            st.subheader("市场类型分布")
            
            market_dist = unified_data['市场类型'].value_counts()
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                fig_pie = px.pie(
                    values=market_dist.values,
                    names=market_dist.index,
                    title="市场类型分布"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                st.dataframe(
                    pd.DataFrame({
                        '市场类型': market_dist.index,
                        '记录数': market_dist.values,
                        '占比': (market_dist.values / market_dist.sum() * 100).round(1)
                    }),
                    use_container_width=True
                )
        
        # Sales quantity analysis by market
        qty_cols = [col for col in unified_data.columns if 'QTY' in col or '数量' in col]
        if qty_cols and '市场类型' in unified_data.columns:
            st.subheader("各市场销售数量分析")
            
            qty_col = qty_cols[0]
            market_sales = unified_data.groupby('市场类型')[qty_col].sum().sort_values(ascending=False)
            
            fig_bar = px.bar(
                x=market_sales.values,
                y=market_sales.index,
                orientation='h',
                title="各市场销售数量",
                labels={'x': '销售数量', 'y': '市场类型'}
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Time series analysis
        date_cols = [col for col in unified_data.columns if any(k in col.lower() for k in ['date', '日期', 'orderdate'])]
        if date_cols and qty_cols:
            st.subheader("销售趋势分析")
            
            date_col = date_cols[0]
            qty_col = qty_cols[0]
            
            # Filter valid dates
            valid_dates = unified_data[unified_data[date_col].notna()]
            
            if not valid_dates.empty:
                time_sales = valid_dates.groupby([valid_dates[date_col].dt.to_period('M'), '市场类型'])[qty_col].sum().reset_index()
                time_sales[date_col] = time_sales[date_col].astype(str)
                
                fig_line = px.line(
                    time_sales,
                    x=date_col,
                    y=qty_col,
                    color='市场类型',
                    title="月度销售趋势"
                )
                st.plotly_chart(fig_line, use_container_width=True)

def display_data_lineage():
    """Display data lineage and governance information"""
    if not st.session_state.data_processed or not st.session_state.pipeline:
        st.warning("⚠️ 请先上传并处理数据文件")
        return
    
    st.header("📋 数据血缘与治理")
    
    pipeline = st.session_state.pipeline
    
    # Data lineage information
    st.subheader("数据血缘追踪")
    
    for sheet_name, lineage_info in pipeline.lineage_tracker.items():
        with st.expander(f"📊 {sheet_name} 数据血缘", expanded=False):
            st.write(f"**数据源:** {lineage_info['source']}")
            st.write(f"**提取时间:** {lineage_info['extraction_timestamp']}")
            
            if lineage_info['transformations']:
                st.write("**转换步骤:**")
                for i, transform in enumerate(lineage_info['transformations'], 1):
                    st.write(f"{i}. {transform['step']} - {transform['timestamp']}")
                    if 'changes' in transform:
                        st.json(transform['changes'])
                    if 'rules_applied' in transform:
                        for rule in transform['rules_applied']:
                            st.write(f"   - {rule}")

def export_data_section():
    """Handle data export functionality"""
    if not st.session_state.data_processed or not st.session_state.pipeline:
        st.warning("⚠️ 请先上传并处理数据文件")
        return
    
    st.header("📤 数据导出")
    
    pipeline = st.session_state.pipeline
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("导出统一数据集")
        if 'unified_model' in pipeline.data_lake['gold']:
            unified_data = pipeline.data_lake['gold']['unified_model']['data']
            
            # Export to CSV
            csv_buffer = io.StringIO()
            unified_data.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            
            st.download_button(
                label="📥 下载 CSV 文件",
                data=csv_buffer.getvalue(),
                file_name=f"menarini_unified_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
            # Export to Excel
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                unified_data.to_excel(writer, sheet_name='统一数据集', index=False)
                
                # Add summary sheet
                summary_data = {
                    '指标': ['总记录数', '总列数', '处理时间'],
                    '值': [len(unified_data), len(unified_data.columns), datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='数据摘要', index=False)
            
            st.download_button(
                label="📥 下载 Excel 文件",
                data=excel_buffer.getvalue(),
                file_name=f"menarini_unified_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    with col2:
        st.subheader("导出质量报告")
        if hasattr(pipeline, 'data_quality_report') and pipeline.data_quality_report:
            # Export quality report as JSON
            quality_json = json.dumps(pipeline.data_quality_report, indent=2, ensure_ascii=False, default=str)
            
            st.download_button(
                label="📥 下载质量报告 (JSON)",
                data=quality_json,
                file_name=f"data_quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

def main():
    """Main application function"""
    initialize_session_state()
    display_header()
    
    # Sidebar navigation
    st.sidebar.title("🧭 导航菜单")
    
    menu_options = [
        "📁 数据上传",
        "📈 数据概览",
        "🔍 质量评估",
        "🏪 市场分析",
        "📋 数据治理",
        "📤 数据导出"
    ]
    
    selected_menu = st.sidebar.radio("选择功能:", menu_options)
    
    # Main content area
    if selected_menu == "📁 数据上传":
        file_upload_section()
    elif selected_menu == "📈 数据概览":
        display_data_overview()
    elif selected_menu == "🔍 质量评估":
        display_data_quality()
    elif selected_menu == "🏪 市场分析":
        display_market_analysis()
    elif selected_menu == "📋 数据治理":
        display_data_lineage()
    elif selected_menu == "📤 数据导出":
        export_data_section()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Menarini Asia Pacific**")
    st.sidebar.markdown("Data & AI Engineering Team")
    st.sidebar.markdown(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
