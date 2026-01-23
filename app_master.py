#!/usr/bin/env python3
"""
Flask web application for grade merging (Master Gradebook Mode).
Provides a web interface for merging grades from a master gradebook into lecture gradebooks.
"""

from flask import Flask, request, jsonify, send_file, render_template_string
from werkzeug.utils import secure_filename
import os
import tempfile
from pathlib import Path
import pandas as pd
from io import BytesIO
import zipfile
import re

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Store uploaded files in temporary directory
UPLOAD_FOLDER = tempfile.mkdtemp()
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def sort_assignment_columns(df):
    """Sort assignment columns by week number, then by assignment type."""
    assignment_cols = []
    non_assignment_cols = []
    
    assignment_type_order = {'PA': 0, 'CA': 1, 'IL': 2, 'OL': 3}
    
    for col in df.columns:
        match = re.match(r'W(\d+)\s+(PA|CA|IL|OL)', col.strip(), re.IGNORECASE)
        if match:
            week_num = int(match.group(1))
            assignment_type = match.group(2).upper()
            type_order = assignment_type_order.get(assignment_type, 99)
            assignment_cols.append((col, week_num, type_order))
        else:
            non_assignment_cols.append(col)
    
    assignment_cols.sort(key=lambda x: (x[1], x[2]))
    sorted_assignment_names = [col[0] for col in assignment_cols]
    
    new_column_order = non_assignment_cols + sorted_assignment_names
    
    return df[new_column_order]


def normalize_student_id(student_id):
    """
    Normalize student ID to a string, handling float/int conversions.
    
    Examples:
        14788528.0 -> "14788528"
        14788528 -> "14788528"
        "14788528" -> "14788528"
    """
    if pd.isna(student_id):
        return None
    
    id_str = str(student_id).strip()
    
    if '.' in id_str:
        try:
            id_float = float(id_str)
            if id_float == int(id_float):
                return str(int(id_float))
        except (ValueError, OverflowError):
            pass
    
    return id_str


# Import all helper functions from merge_grades.py
def find_student_id_column(df):
    """Find the student ID column in a DataFrame."""
    patterns = ['student id', 'studentid', 'student_id', 'sid', 'id', 'student number', 'student_number']
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '')
            if pattern_clean in col_lower or col_lower in pattern_clean:
                return col
    return None


def find_email_column(df):
    """Find the SCHOOL email column in a DataFrame. Only looks for 'School email' specifically."""
    patterns = ['school email', 'schoolemail', 'school_email']
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col
    return None


def find_username_column(df):
    """Find the username column in a DataFrame."""
    patterns = ['username', 'user name', 'user_name']
    for col in df.columns:
        col_lower = col.lower().replace(' ', '').replace('_', '').replace('-', '')
        for pattern in patterns:
            pattern_clean = pattern.replace(' ', '').replace('_', '').replace('-', '')
            if pattern_clean == col_lower:
                return col
    return None


def extract_username_from_email(email):
    """Extract username from email address."""
    if pd.isna(email) or not email or '@' not in str(email):
        return ''
    return str(email).split('@')[0].strip().lower()


def translate_assignment_name(col_name):
    """Translate full assignment names to abbreviated form."""
    match = re.match(r'Week\s+(\d+)\s+(.+?)(?:\s*\([\d.]+\))?$', col_name.strip())
    if not match:
        return col_name
    
    week_num = match.group(1)
    assignment_type = match.group(2).strip()
    
    type_map = {
        'Participation Activities': 'PA',
        'In-Lab Labs': 'IL',
        'Challenge Activities': 'CA',
        'Out-of-Lab Labs': 'OL'
    }
    
    for full_name, abbrev in type_map.items():
        if full_name.lower() == assignment_type.lower():
            return f"W{week_num} {abbrev}"
    
    return col_name


def clean_assignment_name(col_name):
    """Clean assignment column name by removing metadata."""
    if '[' in col_name:
        col_name = col_name.split('[')[0]
    return col_name.rstrip()


def is_gradeable_assignment(col_name):
    """
    Check if a column represents a gradeable assignment.
    Only returns True for columns matching the Week N pattern.
    """
    import re
    
    # Only accept columns that match "Week N ..." pattern
    if re.match(r'Week\s+\d+\s+', col_name.strip(), re.IGNORECASE):
        return True
    
    # Also accept already-abbreviated forms like "W1 PA", "W2 IL", etc.
    if re.match(r'W\d+\s+(PA|IL|CA|OL)', col_name.strip(), re.IGNORECASE):
        return True
    
    return False


def normalize_column_name(name):
    """Normalize column name for fuzzy matching."""
    return name.lower().replace('_', ' ').replace('-', ' ').strip()


def find_matching_column(target_col, column_list):
    """Find matching column in a list of columns."""
    cleaned_target = clean_assignment_name(target_col)
    normalized_target = normalize_column_name(cleaned_target)
    
    for col in column_list:
        cleaned_col = clean_assignment_name(col)
        if normalize_column_name(cleaned_col) == normalized_target:
            return col
    
    for col in column_list:
        cleaned_col = clean_assignment_name(col)
        normalized_col = normalize_column_name(cleaned_col)
        if normalized_target in normalized_col or normalized_col in normalized_target:
            return col
    
    return None


def load_csv_robust(filepath):
    """Load CSV with robust handling of various formats."""
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            try:
                df = pd.read_csv(filepath, encoding=encoding, index_col=False,
                                skipinitialspace=True, on_bad_lines='warn')
            except TypeError:
                df = pd.read_csv(filepath, encoding=encoding, index_col=False,
                                skipinitialspace=True, error_bad_lines=False, warn_bad_lines=True)
            
            df.columns = df.columns.str.strip()
            
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                df = df.drop(columns=unnamed_cols)
            
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.strip()
            return df
        except (UnicodeDecodeError, Exception):
            continue
    
    raise ValueError(f"Could not read {filepath} with any standard encoding")


def merge_grades_from_master(lecture_files, master_file):
    """Merge grades from master gradebook into lecture gradebooks."""
    results = {
        'updated_dataframes': {},
        'stats': {},
        'students_not_found': [],
        'orphaned_rows': [],
        'failed_id_match_rows': [],
        'new_columns': [],
        'matched_students': set(),
        'match_methods': {'by_id': 0, 'by_email': 0}
    }
    
    all_master_students = set()
    master_df = load_csv_robust(master_file)
    
    master_id_col = find_student_id_column(master_df)
    master_email_col = find_email_column(master_df)
    
    if not master_id_col and not master_email_col:
        raise ValueError("Could not find Student ID or Email column in master gradebook")
    
    matched_master_indices = set()
    failed_id_match_details = {}
    
    lecture_data = {}
    for filepath in lecture_files:
        df = load_csv_robust(filepath)
        lecture_data[Path(filepath).name] = df
    
    for lecture_name, lecture_df in lecture_data.items():
        lecture_id_col = find_student_id_column(lecture_df)
        lecture_username_col = find_username_column(lecture_df)
        
        if not lecture_id_col and not lecture_username_col:
            continue
        
        student_id_map = {}
        student_username_map = {}
        
        for idx, row in lecture_df.iterrows():
            if lecture_id_col:
                student_id = row[lecture_id_col]
                student_id_str = normalize_student_id(student_id)
                if student_id_str:
                    student_id_map[student_id_str] = idx
            
            if lecture_username_col:
                username = row[lecture_username_col]
                if pd.notna(username):
                    username_str = str(username).strip().lower()
                    student_username_map[username_str] = idx
        
        grades_updated = 0
        columns_added = 0
        matched_by_id = 0
        matched_by_email = 0
        
        for master_idx, master_row in master_df.iterrows():
            master_student_id = None
            if master_id_col:
                master_student_id = normalize_student_id(master_row[master_id_col])
            
            master_username = None
            if master_email_col:
                master_email = master_row[master_email_col]
                master_username = extract_username_from_email(master_email)
            
            identifier = master_student_id or master_username or "unknown"
            all_master_students.add(identifier)
            
            # SEQUENTIAL MATCHING: Try Student ID first, ONLY if that fails try username
            lecture_row_idx = None
            match_method = None
            
            # Step 1: Try matching by student ID
            if master_student_id and master_student_id in student_id_map:
                lecture_row_idx = student_id_map[master_student_id]
                match_method = 'id'
                matched_by_id += 1
            # Step 2: Only if Student ID didn't match, try matching by username
            elif master_username and master_username in student_username_map:
                lecture_row_idx = student_username_map[master_username]
                match_method = 'email'
                matched_by_email += 1
                # Track if student HAD an ID but it didn't match
                if master_student_id:
                    # Get the correct ID from the matched lecture row
                    matched_lecture_id = lecture_df.at[lecture_row_idx, lecture_id_col] if lecture_id_col else None
                    matched_lecture_id = normalize_student_id(matched_lecture_id) if matched_lecture_id else "N/A"
                    
                    # Get student name from master
                    last_name = ""
                    first_name = ""
                    for col in master_df.columns:
                        if 'last' in col.lower() and 'name' in col.lower():
                            last_name = master_row[col] if pd.notna(master_row[col]) else ""
                        if 'first' in col.lower() and 'name' in col.lower():
                            first_name = master_row[col] if pd.notna(master_row[col]) else ""
                    master_name = f"{last_name}, {first_name}".strip(", ")
                    
                    failed_id_match_details[master_idx] = {
                        'Student Name': master_name,
                        'Email': master_email,
                        'Apparent ID (Master)': master_student_id,
                        'Matched ID (Lecture)': matched_lecture_id
                    }
            
            if lecture_row_idx is None:
                continue
            
            # Mark this master row as matched
            matched_master_indices.add(master_idx)
            results['matched_students'].add(identifier)
            if match_method == 'id':
                results['match_methods']['by_id'] += 1
            else:
                results['match_methods']['by_email'] += 1
            
            # Transfer grades for ONLY assignment columns
            for master_col in master_df.columns:
                # Skip identification columns
                if (master_col == master_id_col or 
                    master_col == master_email_col or
                    'name' in master_col.lower() or
                    'email' in master_col.lower() or
                    'section' in master_col.lower() or
                    'class' in master_col.lower()):
                    continue
                
                # ONLY process assignment columns (Week N pattern)
                if not is_gradeable_assignment(master_col):
                    continue
                
                grade = master_row[master_col]
                if pd.isna(grade) or grade == '':
                    continue
                
                abbreviated_col = translate_assignment_name(master_col)
                lecture_col = find_matching_column(abbreviated_col, lecture_df.columns)
                
                if lecture_col is None:
                    lecture_col = abbreviated_col
                    lecture_df[lecture_col] = ''
                    columns_added += 1
                    if lecture_col not in results['new_columns']:
                        results['new_columns'].append(lecture_col)
                
                lecture_df.at[lecture_row_idx, lecture_col] = grade
                grades_updated += 1
        
        results['updated_dataframes'][lecture_name] = sort_assignment_columns(lecture_df)
        results['stats'][lecture_name] = {
            'grades_updated': grades_updated,
            'columns_added': columns_added,
            'matched_by_id': matched_by_id,
            'matched_by_email': matched_by_email
        }
    
    results['students_not_found'] = sorted(list(all_master_students - results['matched_students']))
    
    # Collect full rows for orphaned students
    orphaned_mask = ~master_df.index.isin(matched_master_indices)
    results['orphaned_rows'] = master_df[orphaned_mask]
    
    # Create focused failed ID matches report
    if failed_id_match_details:
        results['failed_id_match_rows'] = pd.DataFrame.from_dict(
            failed_id_match_details,
            orient='index'
        ).reset_index(drop=True)
    else:
        results['failed_id_match_rows'] = pd.DataFrame()
    
    return results


@app.route('/')
def index():
    """Serve the main page."""
    return render_template_string(HTML_TEMPLATE)


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads."""
    try:
        lecture_files = request.files.getlist('lecture')
        master_file = request.files.get('master')
        
        if not lecture_files or not master_file:
            return jsonify({'error': 'Please upload both lecture and master gradebook files'}), 400
        
        # Clear previous uploads
        for filename in os.listdir(UPLOAD_FOLDER):
            filepath = os.path.join(UPLOAD_FOLDER, filename)
            if os.path.isfile(filepath):
                os.unlink(filepath)
        
        # Save lecture files
        lecture_paths = []
        for file in lecture_files:
            if file.filename:
                filename = secure_filename(file.filename)
                filepath = os.path.join(UPLOAD_FOLDER, 'lecture_' + filename)
                file.save(filepath)
                lecture_paths.append(filepath)
        
        # Save master file
        master_filename = secure_filename(master_file.filename)
        master_path = os.path.join(UPLOAD_FOLDER, 'master_' + master_filename)
        master_file.save(master_path)
        
        # Merge grades
        results = merge_grades_from_master(lecture_paths, master_path)
        
        # Save merged files
        merged_files = []
        for lecture_name, df in results['updated_dataframes'].items():
            unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
            if unnamed_cols:
                df = df.drop(columns=unnamed_cols)
            
            output_name = lecture_name.replace('.csv', '_merged.csv')
            output_path = os.path.join(UPLOAD_FOLDER, output_name)
            df.to_csv(output_path, index=False)
            merged_files.append(output_name)
        
        # Save orphaned students file
        has_orphaned = False
        if len(results['orphaned_rows']) > 0:
            orphaned_path = os.path.join(UPLOAD_FOLDER, 'orphaned_students.csv')
            results['orphaned_rows'].to_csv(orphaned_path, index=False)
            merged_files.append('orphaned_students.csv')
            has_orphaned = True
        
        # Save failed ID matches file
        has_failed_id = False
        if len(results['failed_id_match_rows']) > 0:
            failed_id_path = os.path.join(UPLOAD_FOLDER, 'failed_id_matches.csv')
            results['failed_id_match_rows'].to_csv(failed_id_path, index=False)
            merged_files.append('failed_id_matches.csv')
            has_failed_id = True
        
        return jsonify({
            'success': True,
            'stats': results['stats'],
            'matched_students': len(results['matched_students']),
            'match_by_id': results['match_methods']['by_id'],
            'match_by_email': results['match_methods']['by_email'],
            'students_not_found': results['students_not_found'],
            'orphaned_count': len(results['orphaned_rows']),
            'failed_id_count': len(results['failed_id_match_rows']),
            'new_columns': results['new_columns'],
            'merged_files': merged_files,
            'has_orphaned': has_orphaned,
            'has_failed_id': has_failed_id
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/download/<filename>')
def download_file(filename):
    """Download a merged file."""
    filepath = os.path.join(UPLOAD_FOLDER, secure_filename(filename))
    if os.path.exists(filepath):
        return send_file(filepath, as_attachment=True, download_name=filename)
    return jsonify({'error': 'File not found'}), 404


@app.route('/download_all')
def download_all():
    """Download all merged files as a zip."""
    try:
        memory_file = BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            for filename in os.listdir(UPLOAD_FOLDER):
                if filename.endswith('_merged.csv'):
                    filepath = os.path.join(UPLOAD_FOLDER, filename)
                    zf.write(filepath, filename)
        
        memory_file.seek(0)
        return send_file(
            memory_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name='merged_gradebooks.zip'
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# HTML Template
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Grade Merger - Master Gradebook</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 2rem;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 1rem;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            padding: 2rem;
        }
        
        h1 {
            color: #333;
            margin-bottom: 0.5rem;
            font-size: 2rem;
        }
        
        .subtitle {
            color: #666;
            margin-bottom: 2rem;
        }
        
        .drop-zones {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            margin-bottom: 2rem;
        }
        
        .drop-zone {
            border: 3px dashed #ddd;
            border-radius: 1rem;
            padding: 3rem 2rem;
            text-align: center;
            transition: all 0.3s;
            cursor: pointer;
            background: #fafafa;
        }
        
        .drop-zone:hover {
            border-color: #667eea;
            background: #f0f4ff;
        }
        
        .drop-zone.dragover {
            border-color: #667eea;
            background: #e8edff;
            transform: scale(1.02);
        }
        
        .drop-zone h2 {
            color: #333;
            margin-bottom: 1rem;
            font-size: 1.2rem;
        }
        
        .drop-zone p {
            color: #666;
            margin-bottom: 0.5rem;
        }
        
        .icon {
            font-size: 3rem;
            margin-bottom: 1rem;
        }
        
        .file-list {
            margin-top: 1rem;
            text-align: left;
        }
        
        .file-item {
            background: white;
            padding: 0.5rem 1rem;
            margin: 0.5rem 0;
            border-radius: 0.5rem;
            border: 1px solid #e0e0e0;
            font-size: 0.9rem;
            color: #555;
        }
        
        .controls {
            display: flex;
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        button {
            flex: 1;
            padding: 1rem 2rem;
            border: none;
            border-radius: 0.5rem;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }
        
        .btn-primary {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        .btn-primary:hover:not(:disabled) {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        }
        
        .btn-primary:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .btn-secondary {
            background: white;
            color: #667eea;
            border: 2px solid #667eea;
        }
        
        .btn-secondary:hover {
            background: #f0f4ff;
        }
        
        .status {
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
            display: none;
        }
        
        .status.show {
            display: block;
        }
        
        .status.info {
            background: #e3f2fd;
            color: #1976d2;
            border: 1px solid #90caf9;
        }
        
        .status.success {
            background: #e8f5e9;
            color: #388e3c;
            border: 1px solid #81c784;
        }
        
        .status.error {
            background: #ffebee;
            color: #d32f2f;
            border: 1px solid #ef5350;
        }
        
        .results {
            display: none;
            border-top: 2px solid #f0f0f0;
            padding-top: 2rem;
        }
        
        .results.show {
            display: block;
        }
        
        .results h2 {
            color: #333;
            margin-bottom: 1rem;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        
        .stat-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 1.5rem;
            border-radius: 0.5rem;
        }
        
        .stat-card h3 {
            font-size: 0.9rem;
            opacity: 0.9;
            margin-bottom: 0.5rem;
        }
        
        .stat-card p {
            font-size: 1.5rem;
            font-weight: bold;
        }
        
        .stat-card .detail {
            font-size: 0.85rem;
            opacity: 0.9;
            margin-top: 0.5rem;
        }
        
        .info-box {
            background: #fff3cd;
            border: 1px solid #ffc107;
            color: #856404;
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
        
        .warning-box {
            background: #f8d7da;
            border: 1px solid #f5c6cb;
            color: #721c24;
            padding: 1rem;
            border-radius: 0.5rem;
            margin-bottom: 1rem;
        }
        
        .download-section {
            margin-top: 2rem;
        }
        
        .download-btn {
            display: block;
            width: 100%;
            padding: 1rem;
            margin: 0.5rem 0;
            background: white;
            border: 2px solid #667eea;
            border-radius: 0.5rem;
            color: #667eea;
            text-decoration: none;
            font-weight: 600;
            text-align: center;
            transition: all 0.3s;
        }
        
        .download-btn:hover {
            background: #667eea;
            color: white;
            transform: translateX(5px);
        }
        
        .loading {
            display: none;
            text-align: center;
            padding: 2rem;
        }
        
        .loading.show {
            display: block;
        }
        
        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #667eea;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 1rem;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        @media (max-width: 768px) {
            .drop-zones {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎓 Grade Merger</h1>
        <p class="subtitle">Transfer grades from master gradebook to lecture gradebooks with dual ID/email matching</p>
        
        <div id="status" class="status"></div>
        
        <div class="drop-zones">
            <div class="drop-zone" id="lectureZone">
                <div class="icon">📘</div>
                <h2>Lecture Gradebooks</h2>
                <p>Drag & drop CSV files here</p>
                <p style="font-size: 0.85rem; color: #999;">Sections A, B, etc.</p>
                <input type="file" id="lectureInput" accept=".csv" multiple style="display: none;">
                <div id="lectureFiles" class="file-list"></div>
            </div>
            
            <div class="drop-zone" id="masterZone">
                <div class="icon">📊</div>
                <h2>Master Gradebook</h2>
                <p>Drag & drop CSV file here</p>
                <p style="font-size: 0.85rem; color: #999;">Single master file with all students</p>
                <input type="file" id="masterInput" accept=".csv" style="display: none;">
                <div id="masterFile" class="file-list"></div>
            </div>
        </div>
        
        <div class="controls">
            <button class="btn-primary" id="mergeBtn" disabled>Merge Grades</button>
            <button class="btn-secondary" id="clearBtn">Clear All</button>
        </div>
        
        <div id="loading" class="loading">
            <div class="spinner"></div>
            <p>Merging grades...</p>
        </div>
        
        <div id="results" class="results"></div>
    </div>
    
    <script>
        let lectureFiles = [];
        let masterFile = null;
        
        const lectureZone = document.getElementById('lectureZone');
        const masterZone = document.getElementById('masterZone');
        const lectureInput = document.getElementById('lectureInput');
        const masterInput = document.getElementById('masterInput');
        
        function setupMultiFileZone(zone, input, fileArray, displayId) {
            zone.addEventListener('click', () => input.click());
            
            zone.addEventListener('dragover', (e) => {
                e.preventDefault();
                zone.classList.add('dragover');
            });
            
            zone.addEventListener('dragleave', () => {
                zone.classList.remove('dragover');
            });
            
            zone.addEventListener('drop', (e) => {
                e.preventDefault();
                zone.classList.remove('dragover');
                const files = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
                fileArray.push(...files);
                displayFiles(fileArray, displayId);
                updateMergeButton();
            });
            
            input.addEventListener('change', (e) => {
                const files = Array.from(e.target.files);
                fileArray.push(...files);
                displayFiles(fileArray, displayId);
                updateMergeButton();
            });
        }
        
        function setupSingleFileZone(zone, input, displayId) {
            zone.addEventListener('click', () => input.click());
            
            zone.addEventListener('dragover', (e) => {
                e.preventDefault();
                zone.classList.add('dragover');
            });
            
            zone.addEventListener('dragleave', () => {
                zone.classList.remove('dragover');
            });
            
            zone.addEventListener('drop', (e) => {
                e.preventDefault();
                zone.classList.remove('dragover');
                const files = Array.from(e.dataTransfer.files).filter(f => f.name.endsWith('.csv'));
                if (files.length > 0) {
                    masterFile = files[0];
                    displaySingleFile(masterFile, displayId);
                    updateMergeButton();
                }
            });
            
            input.addEventListener('change', (e) => {
                if (e.target.files.length > 0) {
                    masterFile = e.target.files[0];
                    displaySingleFile(masterFile, displayId);
                    updateMergeButton();
                }
            });
        }
        
        function displayFiles(files, displayId) {
            const container = document.getElementById(displayId);
            container.innerHTML = files.map(f => 
                `<div class="file-item">📄 ${f.name}</div>`
            ).join('');
        }
        
        function displaySingleFile(file, displayId) {
            const container = document.getElementById(displayId);
            container.innerHTML = `<div class="file-item">📄 ${file.name}</div>`;
        }
        
        function updateMergeButton() {
            const mergeBtn = document.getElementById('mergeBtn');
            mergeBtn.disabled = lectureFiles.length === 0 || !masterFile;
        }
        
        function showStatus(message, type) {
            const status = document.getElementById('status');
            status.textContent = message;
            status.className = `status ${type} show`;
        }
        
        setupMultiFileZone(lectureZone, lectureInput, lectureFiles, 'lectureFiles');
        setupSingleFileZone(masterZone, masterInput, 'masterFile');
        
        document.getElementById('mergeBtn').addEventListener('click', async () => {
            const formData = new FormData();
            lectureFiles.forEach(f => formData.append('lecture', f));
            formData.append('master', masterFile);
            
            document.getElementById('loading').classList.add('show');
            document.getElementById('results').classList.remove('show');
            showStatus('Processing files...', 'info');
            
            try {
                const response = await fetch('/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const data = await response.json();
                document.getElementById('loading').classList.remove('show');
                
                if (data.error) {
                    showStatus(data.error, 'error');
                    return;
                }
                
                showStatus('Merge completed successfully!', 'success');
                displayResults(data);
            } catch (error) {
                document.getElementById('loading').classList.remove('show');
                showStatus('Error: ' + error.message, 'error');
            }
        });
        
        document.getElementById('clearBtn').addEventListener('click', () => {
            lectureFiles = [];
            masterFile = null;
            document.getElementById('lectureFiles').innerHTML = '';
            document.getElementById('masterFile').innerHTML = '';
            document.getElementById('results').classList.remove('show');
            document.getElementById('status').classList.remove('show');
            updateMergeButton();
        });
        
        function displayResults(data) {
            const resultsDiv = document.getElementById('results');
            
            let html = '<h2>✅ Merge Results</h2>';
            
            html += '<div class="stats-grid">';
            html += `<div class="stat-card">
                <h3>Students Matched</h3>
                <p>${data.matched_students}</p>
                <div class="detail">ID: ${data.match_by_id} | Email: ${data.match_by_email}</div>
            </div>`;
            
            Object.entries(data.stats).forEach(([section, stats]) => {
                html += `<div class="stat-card">
                    <h3>${section}</h3>
                    <p>${stats.grades_updated} grades</p>
                    <div class="detail">${stats.columns_added} new columns</div>
                    <div class="detail">ID: ${stats.matched_by_id} | Email: ${stats.matched_by_email}</div>
                </div>`;
            });
            html += '</div>';
            
            if (data.new_columns.length > 0) {
                html += '<div class="info-box">';
                html += '<strong>New Columns Created:</strong><br>';
                html += data.new_columns.join(', ');
                html += '</div>';
            }
            
            if (data.students_not_found.length > 0) {
                html += '<div class="warning-box">';
                html += '<strong>Students Not Found in Lecture Sections:</strong><br>';
                html += data.students_not_found.slice(0, 10).join(', ');
                if (data.students_not_found.length > 10) {
                    html += ` ... and ${data.students_not_found.length - 10} more`;
                }
                html += '<br><br>';
                if (data.has_orphaned) {
                    html += `📄 Full data for ${data.orphaned_count} orphaned students saved to <strong>orphaned_students.csv</strong>`;
                }
                html += '</div>';
            }
            
            if (data.has_failed_id) {
                html += '<div class="info-box">';
                html += '<strong>⚠️ ID Mismatches Detected:</strong><br>';
                html += `${data.failed_id_count} students matched by username but had wrong Student ID in master gradebook.<br><br>`;
                html += `📄 Full data saved to <strong>failed_id_matches.csv</strong> - review and correct IDs in master`;
                html += '</div>';
            }
            
            html += '<div class="download-section">';
            html += '<h3>Download Updated Files:</h3>';
            data.merged_files.forEach(filename => {
                html += `<a href="/download/${filename}" class="download-btn">⬇️ ${filename}</a>`;
            });
            html += '<a href="/download_all" class="download-btn" style="background: #667eea; color: white; margin-top: 1rem;">⬇️ Download All as ZIP</a>';
            html += '</div>';
            
            resultsDiv.innerHTML = html;
            resultsDiv.classList.add('show');
        }
    </script>
</body>
</html>
'''


if __name__ == '__main__':
    print("🎓 Grade Merger Web App (Master Gradebook Mode)")
    print("=" * 60)
    print("Starting server at http://localhost:5000")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    app.run(debug=True, port=5000)
