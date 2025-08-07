# VisaPath AI

An intelligent UK visa sponsorship tracking and analysis system that helps job seekers identify visa-sponsoring employers and match with relevant opportunities.

## Overview

VisaPath AI is a comprehensive platform that:
- Tracks and analyzes UK visa sponsor registrations
- Matches job seekers with visa-sponsoring employers
- Provides real-time updates on sponsor status changes
- Integrates with job boards to identify visa-friendly opportunities
- Uses AI to enhance job matching and application processes

## Features

- **Sponsor Registry Tracking**: Daily updates from UK Home Office sponsor register
- **Job Matching**: Intelligent matching between job seekers and visa sponsors
- **Data Analytics**: Comprehensive analysis of sponsorship trends and patterns
- **Neo4j Graph Database**: Advanced relationship mapping between sponsors, jobs, and industries
- **AI-Powered Insights**: Machine learning models for predicting sponsorship likelihood
- **Automated ETL Pipeline**: Daily data extraction, transformation, and loading
- **Streamlit Dashboard**: Interactive web interface for data exploration

## Tech Stack

- **Backend**: Python 3.9+
- **Database**: Neo4j Graph Database
- **Frontend**: Streamlit
- **Data Processing**: Pandas, NumPy
- **Machine Learning**: Scikit-learn, TensorFlow
- **API Integration**: Adzuna Job API
- **ETL**: Custom Python pipelines
- **Version Control**: Git

## Project Structure

```
visapath-ai/
├── data/                   # Raw and processed data files
├── etl/                    # ETL pipelines and scripts
├── neo4j/                  # Neo4j database schemas and queries
├── notebooks/              # Jupyter notebooks for analysis
├── pipeline/               # Data processing pipelines
├── prompts/                # AI prompt templates
├── scripts/                # Utility scripts
├── streamlit_app/          # Streamlit application
└── requirements.txt        # Python dependencies
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/victoropp/visapath-ai.git
cd visapath-ai
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize Neo4j database:
```bash
python etl/etl_neo4j_load.py
```

## Usage

### Running the Streamlit App
```bash
streamlit run streamlit_app/app.py
```

### Updating Sponsor Data
```bash
python etl/etl_sponsor_register.py
```

### Loading Job Data
```bash
python pipeline/load_jobads.py
```

## Data Sources

- **UK Visa Sponsor Register**: Official UK Home Office data
- **Job Listings**: Adzuna API
- **Company Information**: Various public APIs

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions or support, please open an issue on GitHub.

## Acknowledgments

- UK Home Office for providing public sponsor data
- Adzuna for job listing API access
- Neo4j community for graph database support