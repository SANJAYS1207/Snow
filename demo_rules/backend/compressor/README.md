# LLM Explainer - AI-Powered Data Quality Insights

## Overview
The LLM Explainer uses Azure OpenAI to generate business-focused explanations for data quality findings across 8 dimensions, converting technical metrics into actionable insights.

## Features
✅ Azure OpenAI integration for intelligent analysis  
✅ Parallel processing with ThreadPoolExecutor (5 workers)  
✅ Chunked processing for large datasets (20 findings per chunk)  
✅ Two-stage LLM pipeline: Analysis → Business Formatting  
✅ Automatic dimension score calculation  
✅ Consolidated JSON output with explanations  

## Usage

```bash
python llm_explainer.py
```

## Input Format
`final_report.json` - Contains findings from 8 quality dimensions:

```json
{
  "Completeness": [
    {
      "column_name": "customer_email",
      "percentage": 65,
      "issues_count": 350
    }
  ],
  "Validity": [...],
  ...
}
```

## Output

### `llm_explanations.json`
```json
{
  "overall_quality_score": 78.88,
  "dimension_scores": {
    "Completeness": 78.5,
    "Validity": 66.5
  },
  "explanations": [
    {
      "dimension": "Completeness",
      "explanation": "**Completeness**\n• 35% missing values in customer_email impact marketing campaigns\n• Immediate data collection process review recommended",
      "findings_count": 350
    }
  ],
  "total_dimensions": 8
}
```

## Architecture

### Processing Pipeline
```
llm_explainer.py
├── load_report()                    # Load JSON input
├── calculate_dimension_scores()     # Average percentage per dimension
├── analyze_chunk()                  # LLM analysis per chunk (parallel)
├── merge_chunk_analyses()           # Combine multiple chunks
├── format_with_llm()                # Convert to business insights
└── generate_explanations()          # Orchestrate parallel processing
```

### Two-Stage LLM Pipeline

**Stage 1: Technical Analysis**
- Input: Raw findings data (chunked)
- Output: Detailed technical analysis
- Prompt: "Analyze findings covering status, issues, and impact"

**Stage 2: Business Formatting**
- Input: Technical analysis from Stage 1
- Output: 2 bullet points, <50 words
- Prompt: "Convert to business insights with actionable recommendations"

## Configuration

### Azure OpenAI Settings
```python
API_KEY = "your-api-key"
API_VERSION = "2023-07-01-preview"
AZURE_ENDPOINT = "https://your-endpoint.openai.azure.com/"
DEPLOYMENT_NAME = "msgen4o"
```

### Performance Settings
- **Max Workers**: 5 parallel threads
- **Chunk Size**: 20 findings per chunk
- **Max Tokens**: 1024 (analysis), 150 (formatting)
- **Temperature**: 0.3 (consistent outputs)
- **Timeout**: 60 seconds per API call

## Scoring Logic

### Dimension Score
```
Score = Average(percentage values from all findings)
```

### Overall Quality Score
```
Overall = Average(all dimension scores)
```

## Example Output

```
============================================================
DIMENSION SCORES
============================================================

{
  "Completeness": 78.5,
  "Validity": 66.5,
  "Accuracy": 92.3
}

Overall Quality Score: 78.88

============================================================
DATA QUALITY EXPLANATIONS
============================================================

 COMPLETENESS (Score: 78.5% | 350 findings)
------------------------------------------------------------
**Completeness**
• 35% missing customer_email values blocking marketing campaigns
• Implement mandatory field validation at data entry points

============================================================
```

## Requirements
- Python 3.6+
- `requests` library
- Azure OpenAI API access

## Installation
```bash
pip install requests
```

## Error Handling
- API failures return error message in explanation field
- Timeout protection (60s per call)
- Graceful degradation for individual dimension failures

## License
MIT
