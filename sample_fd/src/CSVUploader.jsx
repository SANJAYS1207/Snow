import React, { useState } from 'react';
import axios from 'axios';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell, LineChart, Line } from 'recharts';

const API_BASE = 'http://localhost:5000';

const DataQualityDashboard = () => {
  const [currentStep, setCurrentStep] = useState(0);
  const [data, setData] = useState({ rules: null, allRules: {}, llm: null, recommendations: null, healing: null });
  const [activeTab, setActiveTab] = useState('workflow');
  const [expandedDimension, setExpandedDimension] = useState(null);
  const [expandedColumn, setExpandedColumn] = useState(null);
  const [dimensionProgress, setDimensionProgress] = useState({});
  const [scanData, setScanData] = useState(null);
  const [thresholds, setThresholds] = useState({});
  const [expandedConfigDim, setExpandedConfigDim] = useState(null);

  const dimensionColors = {
    'Completeness': '#2E86AB', 'Uniqueness': '#A23B72', 'Accuracy': '#F18F01',
    'Consistency': '#C73E1D', 'Validity': '#6A994E', 'Timeliness': '#BC4749',
    'Usability': '#2D6A4F', 'Availability': '#1B4965'
  };

  const workflowSteps = [
    { id: 1, name: 'File Upload', description: 'CSV file ingestion' },
    { id: 2, name: 'Configuration', description: 'Set quality thresholds' },
    { id: 3, name: 'Quality Checks', description: '8 dimension checks' },
    { id: 4, name: 'AI Analysis', description: 'Findings explanation' },
    { id: 5, name: 'Recommendations', description: 'Improvement plan' },
    { id: 6, name: 'Healing', description: 'SQL generation' },
    { id: 7, name: 'Results', description: 'Complete report' }
  ];

  const qualityChecks = [
    { name: 'Completeness', description: 'Identifies missing or null values in critical fields' },
    { name: 'Uniqueness', description: 'Detects duplicate records and key violations' },
    { name: 'Accuracy', description: 'Validates data correctness and outlier detection' },
    { name: 'Consistency', description: 'Ensures data format uniformity across records' },
    { name: 'Validity', description: 'Validates data types and domain constraints' },
    { name: 'Timeliness', description: 'Checks data freshness and staleness' },
    { name: 'Usability', description: 'Evaluates data clarity and metadata quality' },
    { name: 'Availability', description: 'Verifies data accessibility and permissions' }
  ];

  const handleUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    
    setCurrentStep(1);
    setData({ rules: null, allRules: {}, llm: null, recommendations: null, healing: null });
    setActiveTab('workflow');
    setDimensionProgress({});

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await axios.post(`${API_BASE}/api/scan`, formData);
      setScanData(res.data);
      
      const initThresholds = {};
      const { rule_mapping } = res.data;
      const defaultT = {
        'Completeness': 90, 'Uniqueness': 95, 'Accuracy': 95,
        'Consistency': 90, 'Validity': 90, 'Timeliness': 90,
        'Usability': 90, 'Availability': 99
      };
      
      rule_mapping.forEach(rule => {
        initThresholds[rule.ruleName] = defaultT[rule.ruleName] || 90;
      });
      setThresholds(initThresholds);
      setCurrentStep(2);
    } catch (error) {
      console.error('Error:', error);
      setCurrentStep(0);
    }
  };

  const startAnalysis = async () => {
    setCurrentStep(3);
    try {
      await axios.post(`${API_BASE}/api/analyze`, {
        filename: scanData.filename,
        thresholds: thresholds
      });
      
      const dimensions = [
        { name: 'completeness', key: 'completeness' },
        { name: 'uniqueness', key: 'uniqueness' },
        { name: 'accuracy', key: 'accuracy' },
        { name: 'consistency', key: 'consistency' },
        { name: 'validity', key: 'validity' },
        { name: 'timeliness', key: 'timeliness' },
        { name: 'usability', key: 'usability' },
        { name: 'availability', key: 'availability' }
      ];
      
      const initialProgress = {};
      dimensions.forEach(dim => {
        initialProgress[dim.key] = 'processing';
      });
      setDimensionProgress(initialProgress);
      
      await Promise.all(dimensions.map(async (dim) => {
        try {
          const res = await pollStage(dim.name);
          setData(prev => ({ ...prev, allRules: { ...prev.allRules, [dim.key]: res.data } }));
          setDimensionProgress(prev => ({ ...prev, [dim.key]: 'completed' }));
        } catch (error) {
          console.error(`Failed to load ${dim.name}:`, error);
          setDimensionProgress(prev => ({ ...prev, [dim.key]: 'completed' }));
        }
      }));
      
      setCurrentStep(4);
      await pollStage('llm_explanations', (res) => {
        setData(prev => ({ ...prev, llm: res.data }));
      }, 600);
      
      setCurrentStep(5);
      await pollStage('recommendations', (res) => {
        setData(prev => ({ ...prev, recommendations: res.data }));
      }, 600);
      
      setCurrentStep(6);
      await pollStage('healing', (res) => {
        setData(prev => ({ ...prev, healing: res.data }));
      }, 600);
      
      setCurrentStep(7);
    } catch (error) {
      console.error('Error:', error);
      setCurrentStep(0);
    }
  };

  const pollStage = async (stageName, callback, maxAttempts = 180) => {
    let attempts = 0;
    while (attempts < maxAttempts) {
      try {
        const res = await axios.get(`${API_BASE}/api/stage/${stageName}`);
        if (res.data.status !== 'not_ready') {
          if (callback) callback(res);
          return res;
        }
      } catch (err) {
        // Continue polling
      }
      await new Promise(r => setTimeout(r, 1000));
      attempts++;
    }
    console.warn(`Timeout waiting for ${stageName} after ${maxAttempts} seconds`);
    throw new Error(`Timeout waiting for ${stageName}`);
  };

  const styles = {
    container: { minHeight: '100vh', background: '#f5f7fa', padding: '20px', fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif" },
    card: { maxWidth: '1600px', margin: '0 auto', background: '#ffffff', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)', overflow: 'hidden' },
    header: { background: '#1B4965', color: '#ffffff', padding: '40px', textAlign: 'center' },
    title: { margin: '0', fontSize: '28px', fontWeight: '600', letterSpacing: '0.5px' },
    subtitle: { margin: '10px 0 0 0', fontSize: '14px', opacity: 0.9 },
    uploadSection: { padding: '60px 40px', textAlign: 'center', background: '#f9fafb' },
    uploadTitle: { fontSize: '20px', fontWeight: '600', marginBottom: '20px', color: '#1B4965' },
    fileInput: { padding: '12px', fontSize: '14px', border: '1px solid #ddd', borderRadius: '4px' },
    tabs: { display: 'flex', gap: '0', borderBottom: '2px solid #e0e0e0', padding: '0 20px' },
    tabButton: { padding: '16px 24px', border: 'none', background: 'transparent', fontSize: '14px', fontWeight: '500', color: '#999', borderBottom: '3px solid transparent', transition: 'all 0.3s' },
    tabButtonActive: { color: '#1B4965', borderBottomColor: '#1B4965' },
    tabButtonDisabled: { color: '#ccc', cursor: 'not-allowed', opacity: 0.5 },
    workflowContainer: { padding: '40px' },
    workflowSteps: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '50px', gap: '15px' },
    stepBox: { flex: 1, textAlign: 'center', padding: '20px', borderRadius: '6px', background: '#f5f7fa', border: '2px solid #e0e0e0', transition: 'all 0.3s', minHeight: '100px', display: 'flex', flexDirection: 'column', justifyContent: 'center' },
    stepBoxActive: { background: '#1B4965', color: 'white', border: '2px solid #1B4965', transform: 'scale(1.05)' },
    stepBoxCompleted: { background: '#6A994E', color: 'white', border: '2px solid #6A994E' },
    stepName: { fontWeight: '600', marginBottom: '8px', fontSize: '14px' },
    stepDesc: { fontSize: '12px', opacity: 0.8 },
    checksGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(380px, 1fr))', gap: '24px', padding: '20px' },
    checkBox: { borderRadius: '6px', overflow: 'hidden', boxShadow: '0 2px 8px rgba(0,0,0,0.08)', border: '1px solid #e0e0e0', alignSelf: 'flex-start' },
    checkHeader: { padding: '18px', color: 'white', cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontWeight: '600', fontSize: '15px' },
    checkContent: { padding: '20px', background: '#fafbfc', height: '400px', overflowY: 'auto' },
    columnRow: { padding: '12px', background: 'white', margin: '8px 0', borderRadius: '4px', borderLeft: '4px solid #ddd', fontSize: '13px' },
    columnName: { fontWeight: '600', marginBottom: '6px', color: '#1B4965' },
    columnDetail: { color: '#666', marginBottom: '4px', lineHeight: '1.4' },
    statusBadge: { display: 'inline-block', padding: '4px 10px', borderRadius: '3px', fontSize: '11px', fontWeight: '600', marginTop: '8px' },
    statusPassed: { background: '#6A994E', color: 'white' },
    statusFailed: { background: '#C73E1D', color: 'white' },
    healingContainer: { padding: '40px' },
    healingGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '30px', marginTop: '30px' },
    healingSection: { background: '#f9fafb', padding: '24px', borderRadius: '6px', border: '1px solid #e0e0e0' },
    healingSectionTitle: { fontSize: '16px', fontWeight: '600', marginBottom: '20px', color: '#1B4965' },
    actionBox: { background: 'white', padding: '16px', marginBottom: '16px', borderRadius: '4px', border: '1px solid #e0e0e0' },
    actionBoxAuto: { borderLeft: '4px solid #6A994E' },
    actionBoxReview: { borderLeft: '4px solid #F18F01' },
    actionTitle: { fontWeight: '600', marginBottom: '8px', fontSize: '14px', color: '#1B4965' },
    actionBadge: { display: 'inline-block', padding: '4px 12px', borderRadius: '3px', fontSize: '11px', fontWeight: '600', marginBottom: '10px' },
    actionBadgeAuto: { background: '#6A994E', color: 'white' },
    actionBadgeReview: { background: '#F18F01', color: 'white' },
    actionDetail: { fontSize: '13px', color: '#666', marginBottom: '8px', lineHeight: '1.5' },
    sqlCode: { display: 'block', background: '#282c34', padding: '12px', borderRadius: '4px', fontSize: '12px', fontFamily: "'Courier New', monospace", color: '#61dafb', marginTop: '10px', overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all' },
    summaryBox: { background: '#f0f4f8', border: '1px solid #2E86AB', borderRadius: '6px', padding: '24px', margin: '20px' },
    summaryTitle: { fontSize: '16px', fontWeight: '600', marginBottom: '12px', color: '#1B4965' },
    summaryText: { fontSize: '13px', color: '#555', lineHeight: '1.8' }
  };

  const dimensions = ['Completeness', 'Uniqueness', 'Accuracy', 'Consistency', 'Validity', 'Timeliness', 'Usability', 'Availability'];
  const lessThan = String.fromCharCode(60);
  const greaterEqual = String.fromCharCode(62) + String.fromCharCode(61);

  // Count completed dimensions
  const completedDimensions = Object.values(dimensionProgress).filter(status => status === 'completed').length;
  
  // Tab enabling: Sequential - each enables only when its data is ready
  const isQualityEnabled = currentStep >= 3 && completedDimensions > 0;
  const isRecommendationsEnabled = data.recommendations !== null;
  const isHealingEnabled = data.healing !== null;
  const isSummaryEnabled = data.recommendations !== null && data.healing !== null;

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <div style={styles.header}>
          <h1 style={styles.title}>Data Quality Analysis Platform</h1>
          <p style={styles.subtitle}>Comprehensive data quality assessment and automated remediation</p>
        </div>

        {currentStep === 0 ? (
          <div style={styles.uploadSection}>
            <h2 style={styles.uploadTitle}>Upload Data File</h2>
            <input type="file" accept=".csv" onChange={handleUpload} style={styles.fileInput} />
            <p style={{marginTop: '20px', fontSize: '13px', color: '#666'}}>Select a CSV file to begin analysis</p>
          </div>
        ) : (
          <>
            <div style={styles.tabs}>
              <button 
                style={{...styles.tabButton, ...(activeTab === 'workflow' ? styles.tabButtonActive : {})}} 
                onClick={() => setActiveTab('workflow')}
              >
                Workflow Progress
              </button>
              <button 
                style={{...styles.tabButton, ...(activeTab === 'quality' ? styles.tabButtonActive : {}), ...(!isQualityEnabled ? styles.tabButtonDisabled : {})}} 
                onClick={() => isQualityEnabled && setActiveTab('quality')}
                disabled={!isQualityEnabled}
              >
                Quality Assessment
              </button>
              <button 
                style={{...styles.tabButton, ...(activeTab === 'recommendations' ? styles.tabButtonActive : {}), ...(!isRecommendationsEnabled ? styles.tabButtonDisabled : {})}} 
                onClick={() => isRecommendationsEnabled && setActiveTab('recommendations')}
                disabled={!isRecommendationsEnabled}
              >
                Recommendations
              </button>
              <button 
                style={{...styles.tabButton, ...(activeTab === 'healing' ? styles.tabButtonActive : {}), ...(!isHealingEnabled ? styles.tabButtonDisabled : {})}} 
                onClick={() => isHealingEnabled && setActiveTab('healing')}
                disabled={!isHealingEnabled}
              >
                Healing Actions
              </button>
              <button 
                style={{...styles.tabButton, ...(activeTab === 'summary' ? styles.tabButtonActive : {}), ...(!isSummaryEnabled ? styles.tabButtonDisabled : {})}} 
                onClick={() => isSummaryEnabled && setActiveTab('summary')}
                disabled={!isSummaryEnabled}
              >
                Summary Report
              </button>
            </div>

            {activeTab === 'workflow' && (
              <div style={styles.workflowContainer}>
                <h2 style={{marginBottom: '30px', color: '#1B4965'}}>Analysis Workflow</h2>
                <div style={styles.workflowSteps}>
                  {workflowSteps.map((step) => {
                    const isCompleted = currentStep > step.id || (currentStep === 7 && step.id === 7);
                    const isActive = currentStep === step.id && step.id !== 7;
                    
                    return (
                      <div key={step.id} style={{
                        ...styles.stepBox,
                        ...(isCompleted ? styles.stepBoxCompleted : {}),
                        ...(isActive ? styles.stepBoxActive : {})
                      }}>
                        <div style={styles.stepName}>{step.name}</div>
                        <div style={styles.stepDesc}>{step.description}</div>
                        {isCompleted && <div style={{marginTop: '10px', fontSize: '12px'}}>✓ Complete</div>}
                        {isActive && <div style={{marginTop: '10px', fontSize: '12px'}}>⟳ Processing...</div>}
                      </div>
                    );
                  })}
                </div>

                {currentStep >= 3 && (
                  <div style={{padding: '20px', background: '#f0f4f8', borderRadius: '6px', marginTop: '30px'}}>
                    <h3 style={{color: '#1B4965', marginBottom: '15px', fontSize: '16px'}}>Quality Check Progress</h3>
                    <div style={{display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '12px'}}>
                      {qualityChecks.map((check) => {
                        const dimKey = check.name.toLowerCase();
                        const isComplete = dimensionProgress[dimKey] === 'completed';
                        const isProcessing = dimensionProgress[dimKey] === 'processing';
                        
                        return (
                          <div key={check.name} style={{
                            padding: '12px',
                            background: isComplete ? '#6A994E' : isProcessing ? '#1B4965' : '#e0e0e0',
                            color: (isComplete || isProcessing) ? 'white' : '#666',
                            borderRadius: '4px',
                            fontSize: '13px',
                            fontWeight: '500',
                            textAlign: 'center',
                            transition: 'all 0.3s'
                          }}>
                            {check.name}
                            {isProcessing && <div style={{fontSize: '11px', marginTop: '4px'}}>⟳</div>}
                            {isComplete && <div style={{fontSize: '11px', marginTop: '4px'}}>✓</div>}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {currentStep === 2 && scanData && (
                  <div style={{padding: '30px', background: 'white', borderRadius: '8px', marginTop: '30px', border: '1px solid #e0e0e0', boxShadow: '0 2px 4px rgba(0,0,0,0.05)'}}>
                    <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px'}}>
                      <div>
                        <h3 style={{color: '#1B4965', margin: '0 0 10px 0', fontSize: '20px'}}>Configure Quality Thresholds</h3>
                        <p style={{color: '#666', margin: '0', fontSize: '14px'}}>Adjust the acceptance thresholds based on the initial column scan metrics before running the full analysis.</p>
                      </div>
                      <button 
                        onClick={startAnalysis}
                        style={{background: '#6A994E', color: 'white', border: 'none', padding: '12px 24px', borderRadius: '6px', fontWeight: 'bold', cursor: 'pointer', fontSize: '15px'}}
                      >
                        Start Analysis
                      </button>
                    </div>
                    
                    <div style={{display: 'flex', flexDirection: 'column', gap: '20px'}}>
                      {dimensions.map((dimName, idx) => {
                        // Find the rule mapping for this dimension if it exists
                        const rule = scanData.rule_mapping.find(r => r.ruleName === dimName) || { ruleName: dimName, columns: [] };
                        const isExpanded = expandedConfigDim === dimName;
                        
                        return (
                          <div key={idx} style={{border: '1px solid #ddd', borderRadius: '8px', overflow: 'hidden', boxShadow: '0 1px 3px rgba(0,0,0,0.02)'}}>
                            <div 
                              style={{
                                display: 'flex', 
                                justifyContent: 'space-between', 
                                alignItems: 'center', 
                                background: isExpanded ? '#f0f4f8' : '#f8f9fa', 
                                padding: '15px 20px', 
                                borderBottom: isExpanded ? '1px solid #ddd' : 'none',
                                cursor: 'pointer',
                                transition: 'background 0.2s'
                              }}
                              onClick={() => setExpandedConfigDim(isExpanded ? null : dimName)}
                            >
                              <div style={{fontWeight: 'bold', color: '#1B4965', display: 'flex', alignItems: 'center', gap: '10px'}}>
                                <span style={{display: 'inline-block', width: '20px', textAlign: 'center'}}>{isExpanded ? '▼' : '▶'}</span>
                                {dimName} Dashboard <span style={{fontSize: '12px', color: '#666', fontWeight: 'normal', marginLeft: '10px'}}>({rule.columns.length} columns)</span>
                              </div>
                              <div 
                                style={{display: 'flex', alignItems: 'center', gap: '10px'}}
                                onClick={(e) => e.stopPropagation()} // Prevent accordion toggle when interacting with input
                              >
                                <span style={{fontSize: '13px', color: '#555', fontWeight: '600'}}>Global Threshold:</span>
                                <input 
                                  type="number" 
                                  min="0" max="100" 
                                  value={thresholds[dimName] ?? 90}
                                  onChange={(e) => {
                                    setThresholds(prev => ({
                                      ...prev,
                                      [dimName]: Number(e.target.value)
                                    }));
                                  }}
                                  style={{width: '60px', padding: '8px', borderRadius: '4px', border: '1px solid #ccc', textAlign: 'right'}} 
                                />
                                <span style={{color: '#666'}}>%</span>
                              </div>
                            </div>
                            
                            {isExpanded && (
                              <div style={{padding: '0 20px', background: 'white'}}>
                                {rule.columns.length > 0 ? (
                                  rule.columns.map((colName, cidx) => {
                                    const colStats = scanData.column_analyses.find(c => c.name === colName) || {};
                                    return (
                                      <div key={cidx} style={{display: 'flex', alignItems: 'center', padding: '15px 0', borderBottom: (cidx < rule.columns.length - 1) ? '1px solid #f0f0f0' : 'none'}}>
                                        <div style={{flex: 1, paddingLeft: '30px'}}>
                                          <div style={{fontWeight: '600', color: '#333', marginBottom: '5px'}}>{colName}</div>
                                          <div style={{fontSize: '12px', color: '#777', display: 'flex', gap: '15px'}}>
                                            <span><b>Type:</b> {colStats.dtype}</span>
                                            <span><b>Fill Rate:</b> {((colStats.fill_rate || 0) * 100).toFixed(1)}% ({colStats.filled}/{colStats.total})</span>
                                            <span><b>Unique:</b> {colStats.unique}</span>
                                          </div>
                                        </div>
                                      </div>
                                    );
                                  })
                                ) : (
                                  <div style={{padding: '20px', textAlign: 'center', color: '#999', fontSize: '14px', fontStyle: 'italic'}}>
                                    No columns categorized under this dimension based on initial scan.
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            )}

            {activeTab === 'quality' && isQualityEnabled && (
              <div>
                <div style={{padding: '20px'}}>
                  <h2 style={{color: '#1B4965', marginBottom: '10px'}}>Quality Assessment Results</h2>
                  <p style={{color: '#666', fontSize: '13px'}}>
                    Showing {completedDimensions} of 8 dimensions completed
                  </p>
                </div>
                <div style={styles.checksGrid}>
                  {dimensions.map(dim => {
                    const dimData = data.allRules[dim.toLowerCase()];
                    const isCompleted = dimensionProgress[dim.toLowerCase()] === 'completed';
                    
                    // Show placeholder for dimensions still processing
                    if (!dimData || !isCompleted) {
                      return (
                        <div key={dim} style={{...styles.checkBox, borderTop: `4px solid ${dimensionColors[dim]}`, opacity: 0.6}}>
                          <div style={{...styles.checkHeader, background: dimensionColors[dim]}}>
                            <div>
                              <div>{dim}</div>
                              <div style={{fontSize: '12px', marginTop: '6px', opacity: 0.9}}>Processing...</div>
                            </div>
                            <span style={{fontSize: '18px'}}>⟳</span>
                          </div>
                        </div>
                      );
                    }
                    
                    const avgScore = Math.round(dimData.reduce((sum, col) => sum + (col.score || 0), 0) / dimData.length);
                    const passedCount = dimData.filter(col => col.status === 'passed').length;
                    const failedCount = dimData.filter(col => col.status === 'failed').length;
                    const isExpanded = expandedDimension === dim;

                    return (
                      <div key={dim} style={{...styles.checkBox, borderTop: `4px solid ${dimensionColors[dim]}`}}>
                        <div 
                          style={{...styles.checkHeader, background: dimensionColors[dim]}}
                          onClick={() => setExpandedDimension(isExpanded ? null : dim)}
                        >
                          <div>
                            <div>{dim}</div>
                            <div style={{fontSize: '12px', marginTop: '6px', opacity: 0.9}}>
                              Score: {avgScore}% | Passed: {passedCount} | Failed: {failedCount}
                            </div>
                          </div>
                          <span style={{fontSize: '18px'}}>{isExpanded ? '−' : '+'}</span>
                        </div>

                        {isExpanded && (
                          <div style={styles.checkContent}>
                            <div style={{marginBottom: '12px', padding: '10px', background: '#e8f4f8', borderRadius: '4px', fontSize: '12px', color: '#1B4965'}}>
                              {qualityChecks.find(q => q.name === dim)?.description}
                            </div>
                            {dimData.map((col, idx) => {
                              const isColExpanded = expandedColumn === `${dim}-${idx}`;
                              const generateSummary = () => {
                                const status = col.status === 'passed' ? 'passed all checks' : 'failed quality checks';
                                const score = col.score;
                                let details = [];
                                
                                if (col.filled_rows !== undefined && col.total_rows !== undefined) {
                                  const fillRate = ((col.filled_rows / col.total_rows) * 100).toFixed(1);
                                  details.push(`has ${col.filled_rows} out of ${col.total_rows} rows filled (${fillRate}% fill rate)`);
                                }
                                if (col.null_rows !== undefined) {
                                  details.push(`contains ${col.null_rows} missing values (${col.null_percentage?.toFixed(2)}%)`);
                                }
                                if (col.duplicates !== undefined && col.duplicates > 0) {
                                  details.push(`has ${col.duplicates} duplicate entries`);
                                }
                                if (col.outlier_count !== undefined && col.outlier_count > 0) {
                                  details.push(`contains ${col.outlier_count} outlier values`);
                                }
                                if (col.invalid_values !== undefined && col.invalid_values > 0) {
                                  details.push(`has ${col.invalid_values} invalid entries`);
                                }
                                
                                const improvement = 100 - score;
                                const improvementText = improvement > 0 ? ` To reach 100% quality, this column needs ${improvement.toFixed(1)}% improvement.` : ' This column meets all quality standards.';
                                
                                return `The column "${col.column_name}" ${status} with a quality score of ${score}%. ${details.length > 0 ? 'Analysis shows it ' + details.join(', ') + '.' : ''}${improvementText}`;
                              };
                              
                              return (
                                <div key={idx}>
                                  <div 
                                    style={{...styles.columnRow, cursor: 'pointer', borderLeft: `4px solid ${col.status === 'passed' ? '#6A994E' : '#C73E1D'}`}}
                                    onClick={() => setExpandedColumn(isColExpanded ? null : `${dim}-${idx}`)}
                                  >
                                    <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                                      <div style={styles.columnName}>{col.column_name}</div>
                                      <div style={{display: 'flex', gap: '8px', alignItems: 'center'}}>
                                        <span style={{fontSize: '12px', color: '#666'}}>{col.score}%</span>
                                        <span style={{...styles.statusBadge, ...(col.status === 'passed' ? styles.statusPassed : styles.statusFailed)}}>
                                          {col.status?.toUpperCase()}
                                        </span>
                                        <span style={{fontSize: '14px', color: '#999'}}>{isColExpanded ? '−' : '+'}</span>
                                      </div>
                                    </div>
                                  </div>
                                  {isColExpanded && (
                                    <div style={{padding: '16px', background: '#f9fafb', margin: '0 8px 8px 8px', borderRadius: '4px', fontSize: '14px', color: '#374151', lineHeight: '1.6', border: '1px solid #e5e7eb'}}>
                                      {generateSummary()}
                                    </div>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {activeTab === 'recommendations' && isRecommendationsEnabled && data.recommendations && (
              <div style={{padding: '40px'}}>
                <h2 style={{color: '#1B4965', marginBottom: '10px'}}>Improvement Recommendations</h2>
                <p style={{color: '#666', fontSize: '13px', marginBottom: '30px'}}>AI-generated recommendations for data quality improvement</p>
                
                <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))', gap: '24px'}}>
                  {(() => {
                    const recText = data.recommendations.recommendations || '';
                    // Split by ### and filter out empty and summary sections
                    const sections = recText.split(/###\s+/).filter(s => {
                      const trimmed = s.trim();
                      return trimmed && 
                             !trimmed.startsWith('[SUMMARY]') && 
                             !trimmed.startsWith('[EXECUTION') &&
                             !trimmed.startsWith('SUMMARY') &&
                             !trimmed.startsWith('EXECUTION');
                    });
                    
                    return sections.map((section, idx) => {
                      const lines = section.trim().split('\n');
                      const dimension = lines[0]?.trim();
                      
                      const extractField = (field) => {
                        // Try with dash first: - **Field**:
                        let line = lines.find(l => l.includes(`- **${field}**`));
                        if (line) {
                          const match = line.match(new RegExp(`-\\s*\\*\\*${field}\\*\\*:?\\s*(.+?)(?:\\s*$|\\s{2,})`));
                          return match ? match[1].trim() : 'N/A';
                        }
                        // Try without dash: **Field**:
                        line = lines.find(l => l.includes(`**${field}**`) && !l.includes(`- **${field}**`));
                        if (line) {
                          const match = line.match(new RegExp(`\\*\\*${field}\\*\\*:?\\s*(.+?)(?:\\s*$|\\s{2,})`));
                          return match ? match[1].trim() : 'N/A';
                        }
                        return 'N/A';
                      };
                      
                      const approach = extractField('Approach');
                      const criticality = extractField('Criticality');
                      const effort = extractField('Effort');
                      const improvement = extractField('Expected Improvement');
                      const sqlFixable = extractField('SQL Fixable');
                      const reason = extractField('Reason');
                      
                      return (
                        <div key={idx} style={{background: 'white', borderRadius: '6px', boxShadow: '0 1px 3px rgba(0,0,0,0.1)', overflow: 'hidden', border: '1px solid #e5e7eb'}}>
                          <div style={{background: '#f9fafb', borderBottom: '1px solid #e5e7eb', padding: '16px 20px'}}>
                            <h3 style={{margin: '0', fontSize: '16px', fontWeight: '600', color: '#1B4965'}}>{dimension}</h3>
                          </div>
                          
                          <div style={{padding: '20px'}}>
                            <div style={{marginBottom: '20px'}}>
                              <div style={{fontSize: '11px', color: '#6b7280', marginBottom: '8px', textTransform: 'uppercase', fontWeight: '600', letterSpacing: '0.5px'}}>Recommended Action</div>
                              <div style={{fontSize: '14px', color: '#374151', lineHeight: '1.6'}}>{approach}</div>
                            </div>
                            
                            {reason !== 'N/A' && (
                              <div style={{marginBottom: '20px', paddingLeft: '12px', borderLeft: '3px solid #e5e7eb'}}>
                                <div style={{fontSize: '13px', color: '#6b7280', lineHeight: '1.5'}}>{reason}</div>
                              </div>
                            )}
                            
                            <div style={{display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '12px', paddingTop: '16px', borderTop: '1px solid #f3f4f6'}}>
                              <div>
                                <div style={{fontSize: '11px', color: '#9ca3af', marginBottom: '4px'}}>Priority</div>
                                <div style={{fontSize: '13px', fontWeight: '600', color: '#374151'}}>{criticality}</div>
                              </div>
                              <div>
                                <div style={{fontSize: '11px', color: '#9ca3af', marginBottom: '4px'}}>Effort</div>
                                <div style={{fontSize: '13px', fontWeight: '600', color: '#374151'}}>{effort}</div>
                              </div>
                              <div>
                                <div style={{fontSize: '11px', color: '#9ca3af', marginBottom: '4px'}}>Improvement</div>
                                <div style={{fontSize: '13px', fontWeight: '600', color: '#059669'}}>{improvement}</div>
                              </div>
                              <div>
                                <div style={{fontSize: '11px', color: '#9ca3af', marginBottom: '4px'}}>Auto-Fix</div>
                                <div style={{fontSize: '13px', fontWeight: '600', color: '#374151'}}>{sqlFixable}</div>
                              </div>
                            </div>
                          </div>
                        </div>
                      );
                    });
                  })()}
                </div>
              </div>
            )}

            {activeTab === 'healing' && isHealingEnabled && data.healing && (
              <div style={styles.healingContainer}>
                <h2 style={{color: '#1B4965', marginBottom: '10px'}}>Automated Remediation</h2>
                <p style={{color: '#666', fontSize: '13px', marginBottom: '30px'}}>SQL scripts for data quality fixes</p>
                
                <div style={styles.healingGrid}>
                  <div style={styles.healingSection}>
                    <div style={styles.healingSectionTitle}>{`Automatic Actions (Confidence ${greaterEqual} 90%)`}</div>
                    {data.healing.auto_actions && data.healing.auto_actions.length > 0 ? (
                      data.healing.auto_actions.map(action => (
                        <div key={action.id} style={{...styles.actionBox, ...styles.actionBoxAuto}}>
                          <span style={{...styles.actionBadge, ...styles.actionBadgeAuto}}>AUTO</span>
                          <div style={styles.actionTitle}>{action.dimension}</div>
                          <div style={styles.actionDetail}>Confidence: {action.confidence}%</div>
                          <div style={styles.actionDetail}>SQL Fixable: {action.sql_queries?.length || 0} | Non-SQL Fixable: {(action.columns?.length || 0) - (action.sql_queries?.length || 0)}</div>
                          <div style={styles.actionDetail}>Columns: {action.columns?.join(', ')}</div>
                          {action.sql_queries && action.sql_queries.length > 0 && (
                            <details style={{marginTop: '10px'}}>
                              <summary style={{cursor: 'pointer', fontWeight: '600', fontSize: '12px', color: '#1B4965', marginBottom: '8px'}}>View SQL ({action.sql_queries.length})</summary>
                              {action.sql_queries.map((sql, i) => (
                                <code key={i} style={styles.sqlCode}>{sql}</code>
                              ))}
                            </details>
                          )}
                        </div>
                      ))
                    ) : (
                      <p style={{color: '#999', fontSize: '13px'}}>No automatic actions available</p>
                    )}
                  </div>

                  <div style={styles.healingSection}>
                    <div style={styles.healingSectionTitle}>{`Review Required (Confidence ${lessThan} 90%)`}</div>
                    {data.healing.review_actions && data.healing.review_actions.length > 0 ? (
                      data.healing.review_actions.map(action => (
                        <div key={action.id} style={{...styles.actionBox, ...styles.actionBoxReview}}>
                          <span style={{...styles.actionBadge, ...styles.actionBadgeReview}}>REVIEW</span>
                          <div style={styles.actionTitle}>{action.dimension}</div>
                          <div style={styles.actionDetail}>Confidence: {action.confidence}%</div>
                          <div style={styles.actionDetail}>SQL Fixable: {action.sql_queries?.length || 0} | Non-SQL Fixable: {(action.columns?.length || 0) - (action.sql_queries?.length || 0)}</div>
                          <div style={styles.actionDetail}>Columns: {action.columns?.join(', ')}</div>
                          {action.sql_queries && action.sql_queries.length > 0 && (
                            <details style={{marginTop: '10px'}}>
                              <summary style={{cursor: 'pointer', fontWeight: '600', fontSize: '12px', color: '#1B4965', marginBottom: '8px'}}>View SQL ({action.sql_queries.length})</summary>
                              {action.sql_queries.map((sql, i) => (
                                <code key={i} style={styles.sqlCode}>{sql}</code>
                              ))}
                            </details>
                          )}
                        </div>
                      ))
                    ) : (
                      <p style={{color: '#999', fontSize: '13px'}}>No review actions available</p>
                    )}
                  </div>
                </div>
              </div>
            )}

            {activeTab === 'summary' && isSummaryEnabled && (
              <div style={{padding: '40px'}}>
                <h2 style={{color: '#1B4965', marginBottom: '30px'}}>Analysis Summary</h2>
                
                {/* Overall Score Card */}
                <div style={{display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '20px', marginBottom: '30px'}}>
                  <div style={{background: '#f0f4f8', padding: '24px', borderRadius: '8px', textAlign: 'center', border: '2px solid #2E86AB'}}>
                    <div style={{fontSize: '14px', color: '#666', marginBottom: '8px'}}>Current Score</div>
                    <div style={{fontSize: '36px', fontWeight: '700', color: data.healing?.current_score < 70 ? '#C73E1D' : data.healing?.current_score < 85 ? '#F18F01' : '#6A994E'}}>
                      {data.healing?.current_score || 0}%
                    </div>
                  </div>
                  <div style={{background: '#f0f4f8', padding: '24px', borderRadius: '8px', textAlign: 'center', border: '2px solid #6A994E'}}>
                    <div style={{fontSize: '14px', color: '#666', marginBottom: '8px'}}>Target Score</div>
                    <div style={{fontSize: '36px', fontWeight: '700', color: '#6A994E'}}>
                      {data.healing?.target_score || 90}%
                    </div>
                  </div>
                  <div style={{background: '#f0f4f8', padding: '24px', borderRadius: '8px', textAlign: 'center', border: '2px solid #F18F01'}}>
                    <div style={{fontSize: '14px', color: '#666', marginBottom: '8px'}}>Improvement Needed</div>
                    <div style={{fontSize: '36px', fontWeight: '700', color: '#F18F01'}}>
                      +{data.healing?.improvement_needed || 0}%
                    </div>
                  </div>
                </div>

                {/* Dimension Scores Chart */}
                <div style={{background: 'white', padding: '24px', borderRadius: '8px', marginBottom: '30px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)'}}>
                  <h3 style={{color: '#1B4965', marginBottom: '20px', fontSize: '18px'}}>Quality Scores by Dimension</h3>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={dimensions.map(dim => {
                      const dimData = data.allRules[dim.toLowerCase()] || [];
                      const avgScore = dimData.length > 0 ? Math.round(dimData.reduce((sum, col) => sum + (col.score || 0), 0) / dimData.length) : 0;
                      const passedCount = dimData.filter(col => col.status === 'passed').length;
                      const failedCount = dimData.filter(col => col.status === 'failed').length;
                      return { name: dim, score: avgScore, passed: passedCount, failed: failedCount };
                    })}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} style={{fontSize: '12px'}} />
                      <YAxis label={{ value: 'Score (%)', angle: -90, position: 'insideLeft' }} />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="score" fill="#2E86AB" name="Quality Score" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Pass/Fail Distribution */}
                <div style={{display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '30px', marginBottom: '30px'}}>
                  <div style={{background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)'}}>
                    <h3 style={{color: '#1B4965', marginBottom: '20px', fontSize: '18px'}}>Overall Pass/Fail Distribution</h3>
                    <ResponsiveContainer width="100%" height={250}>
                      <PieChart>
                        <Pie
                          data={[
                            { name: 'Passed', value: Object.values(data.allRules).flat().filter(c => c.status === 'passed').length },
                            { name: 'Failed', value: Object.values(data.allRules).flat().filter(c => c.status === 'failed').length }
                          ]}
                          cx="50%"
                          cy="50%"
                          labelLine={false}
                          label={({name, percent}) => `${name}: ${(percent * 100).toFixed(0)}%`}
                          outerRadius={80}
                          fill="#8884d8"
                          dataKey="value"
                        >
                          <Cell fill="#6A994E" />
                          <Cell fill="#C73E1D" />
                        </Pie>
                        <Tooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </div>

                  <div style={{background: 'white', padding: '24px', borderRadius: '8px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)'}}>
                    <h3 style={{color: '#1B4965', marginBottom: '20px', fontSize: '18px'}}>Healing Actions Summary</h3>
                    <div style={{display: 'flex', flexDirection: 'column', gap: '16px', marginTop: '30px'}}>
                      <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '16px', background: '#f0f4f8', borderRadius: '6px'}}>
                        <span style={{fontSize: '14px', color: '#666'}}>Automatic Actions</span>
                        <span style={{fontSize: '24px', fontWeight: '700', color: '#6A994E'}}>{data.healing?.auto_actions?.length || 0}</span>
                      </div>
                      <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '16px', background: '#f0f4f8', borderRadius: '6px'}}>
                        <span style={{fontSize: '14px', color: '#666'}}>Review Required</span>
                        <span style={{fontSize: '24px', fontWeight: '700', color: '#F18F01'}}>{data.healing?.review_actions?.length || 0}</span>
                      </div>
                      <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '16px', background: '#f0f4f8', borderRadius: '6px'}}>
                        <span style={{fontSize: '14px', color: '#666'}}>Total SQL Queries</span>
                        <span style={{fontSize: '24px', fontWeight: '700', color: '#1B4965'}}>{data.healing?.summary?.total_sql_queries || 0}</span>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Dimension Details with Before/After Projection */}
                <div style={{background: 'white', padding: '24px', borderRadius: '8px', marginBottom: '30px', boxShadow: '0 2px 8px rgba(0,0,0,0.1)'}}>
                  <h3 style={{color: '#1B4965', marginBottom: '20px', fontSize: '18px'}}>Before vs After Healing (Projected)</h3>
                  <p style={{fontSize: '13px', color: '#666', marginBottom: '16px'}}>Note: Dimensions at 100% cannot improve further. Focus on dimensions below target (90%).</p>
                  <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={(() => {
                      // Parse recommendations to get improvement values
                      const improvementMap = {};
                      if (data.recommendations?.recommendations) {
                        const recText = data.recommendations.recommendations;
                        const sections = recText.split(/###\s+/).filter(s => s.trim());
                        sections.forEach(section => {
                          const lines = section.trim().split('\n');
                          const dimension = lines[0]?.trim();
                          const impLine = lines.find(l => l.includes('**Expected Improvement**'));
                          if (impLine && dimension) {
                            const match = impLine.match(/\d+/g);
                            if (match && match.length > 0) {
                              improvementMap[dimension.toUpperCase()] = Math.max(...match.map(n => parseInt(n)));
                            }
                          }
                        });
                      }
                      
                      return dimensions.map(dim => {
                        const dimData = data.allRules[dim.toLowerCase()] || [];
                        const currentScore = dimData.length > 0 ? Math.round(dimData.reduce((sum, col) => sum + (col.score || 0), 0) / dimData.length) : 0;
                        const improvement = improvementMap[dim.toUpperCase()] || 0;
                        const projectedScore = Math.min(100, currentScore + improvement);
                        return { 
                          name: dim, 
                          current: currentScore, 
                          projected: projectedScore,
                          improvement: improvement,
                          canImprove: currentScore < 100
                        };
                      });
                    })()}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" angle={-45} textAnchor="end" height={100} style={{fontSize: '12px'}} />
                      <YAxis label={{ value: 'Score (%)', angle: -90, position: 'insideLeft' }} />
                      <Tooltip content={({active, payload}) => {
                        if (active && payload && payload.length) {
                          const data = payload[0].payload;
                          const actualImprovement = data.projected - data.current;
                          return (
                            <div style={{background: 'white', padding: '12px', border: '1px solid #e5e7eb', borderRadius: '4px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)'}}>
                              <div style={{fontWeight: '600', marginBottom: '8px'}}>{data.name}</div>
                              <div style={{fontSize: '13px', color: '#C73E1D'}}>Current: {data.current}%</div>
                              <div style={{fontSize: '13px', color: '#6A994E'}}>After Healing: {data.projected}%</div>
                              {data.canImprove ? (
                                <div style={{fontSize: '13px', color: '#666', marginTop: '4px'}}>
                                  Improvement: +{actualImprovement}%
                                  {actualImprovement < data.improvement && <span style={{fontSize: '11px', color: '#999'}}> (capped at 100%)</span>}
                                </div>
                              ) : (
                                <div style={{fontSize: '13px', color: '#999', marginTop: '4px'}}>Already at maximum</div>
                              )}
                            </div>
                          );
                        }
                        return null;
                      }} />
                      <Legend />
                      <Bar dataKey="current" fill="#C73E1D" name="Before Healing" />
                      <Bar dataKey="projected" fill="#6A994E" name="After Healing" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>

                {/* Detailed Summary */}
                <div style={styles.summaryBox}>
                  <div style={styles.summaryTitle}>Detailed Analysis Report</div>
                  <div style={styles.summaryText}>
                    <strong>Total Columns Analyzed:</strong> {Object.values(data.allRules).flat().length}<br/>
                    <strong>Quality Dimensions:</strong> 8 (Completeness, Uniqueness, Accuracy, Consistency, Validity, Timeliness, Usability, Availability)<br/>
                    <strong>Issues Identified:</strong> {Object.values(data.allRules).flat().filter(c => c.status === 'failed').length} columns with quality issues<br/>
                    <strong>Compliant Columns:</strong> {Object.values(data.allRules).flat().filter(c => c.status === 'passed').length}<br/>
                    <br/>
                    <strong>Recommended Actions:</strong><br/>
                    1. Review quality assessment results to understand identified issues<br/>
                    2. Examine healing actions for automated and manual remediation options<br/>
                    3. Execute automatic actions for high-confidence fixes<br/>
                    4. Validate review actions before execution<br/>
                    5. Monitor data quality metrics post-remediation
                  </div>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};

export default DataQualityDashboard;
