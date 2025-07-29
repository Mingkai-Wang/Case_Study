# Menarini Asia Pacific Data Pipeline

## Overview

This repository contains a comprehensive data pipeline solution for Menarini Asia Pacific, designed to handle data ingestion, transformation, visualization, and governance. The application is built using Streamlit as the web interface, with a focus on processing and analyzing market data, including Chinese text support.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

The application follows a multi-layered architecture pattern:

### Frontend Architecture
- **Streamlit Web Application**: Provides an interactive web interface for data visualization and pipeline management
- **Custom CSS Styling**: Enhanced UI with Chinese font support (SimHei, Arial Unicode MS)
- **Responsive Layout**: Wide layout configuration for better data visualization

### Backend Architecture
- **Data Pipeline Module** (`data_pipeline.py`): Core data processing engine implementing ETL operations
- **Visualization Module** (`visualization.py`): Specialized component for creating interactive charts and graphs
- **Layered Data Lake Architecture**: Bronze (raw), Silver (cleaned), Gold (business-ready) data layers

### Data Processing Strategy
- **Automated Data Ingestion**: Handles Excel file processing and data validation
- **Data Transformation Pipeline**: Multi-stage cleaning and enrichment processes
- **Quality Monitoring**: Built-in data quality reporting and lineage tracking

## Key Components

### 1. MenariniDataPipeline Class
- **Purpose**: Central data processing engine
- **Functionality**: Data ingestion, transformation, storage, and governance
- **Architecture Decision**: Class-based design for modularity and state management
- **Benefits**: Encapsulation of business logic, easy testing, and extensibility

### 2. DataVisualization Class
- **Purpose**: Specialized visualization component
- **Technology**: Plotly for interactive charts
- **Features**: Market distribution charts, sales trend analysis
- **Rationale**: Separation of concerns between data processing and presentation

### 3. Streamlit Web Interface
- **Purpose**: User-facing application layer
- **Configuration**: Wide layout with expanded sidebar
- **Styling**: Custom CSS for Chinese character support
- **Benefits**: Rapid prototyping, built-in interactive widgets

## Data Flow

1. **Data Ingestion**: Excel files are processed through the MenariniDataPipeline
2. **Bronze Layer**: Raw data storage with minimal processing
3. **Silver Layer**: Cleaned and validated data
4. **Gold Layer**: Business-ready, enriched data for analytics
5. **Visualization**: Interactive charts and dashboards via Streamlit
6. **Quality Monitoring**: Continuous data quality assessment and lineage tracking

## External Dependencies

### Core Libraries
- **pandas**: Data manipulation and analysis
- **numpy**: Numerical computing
- **plotly**: Interactive visualization
- **streamlit**: Web application framework

### Supporting Libraries
- **matplotlib/seaborn**: Additional plotting capabilities
- **sqlite3**: Local database storage
- **logging**: Application monitoring and debugging

### Rationale for Technology Choices
- **Plotly over Matplotlib**: Interactive visualizations for better user experience
- **Streamlit over Flask/Django**: Rapid development for data applications
- **SQLite**: Lightweight database solution for prototype/development phases

## Deployment Strategy

### Current Architecture
- **Local Development**: Streamlit development server
- **Data Storage**: File-based with SQLite for structured data
- **Configuration**: JSON-based configuration management

### Scalability Considerations
- **Database Migration Path**: SQLite can be replaced with PostgreSQL for production
- **Containerization Ready**: Python-based stack suitable for Docker deployment
- **Cloud Deployment**: Compatible with cloud platforms (AWS, GCP, Azure)

### Monitoring and Logging
- **Built-in Logging**: Comprehensive logging framework
- **Data Quality Tracking**: Automated quality assessment
- **Lineage Tracking**: Data provenance and transformation history

## Development Notes

### Chinese Language Support
- Font configuration for proper Chinese character rendering
- Unicode handling throughout the data pipeline
- Localized chart titles and labels

### Data Lake Architecture
- Three-tier data organization (Bronze, Silver, Gold)
- Supports data governance and quality management
- Enables data versioning and historical analysis

### Extensibility Features
- Configuration-driven pipeline setup
- Modular component design
- Plugin-ready architecture for additional data sources