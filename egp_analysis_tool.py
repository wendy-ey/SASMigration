import zipfile
import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
import glob

# === PATH TO EGP FILE ===
egp_file_path = "insert/your/path/to/file.egp"
output_dir = "egp_extracted"

def extract_sas_from_logs(content):
    """Extract SAS code from log files by parsing the line numbers"""
    if not content:
        return ""
    
    sas_lines = []
    lines = content.split('\n')
    
    for line in lines:
        # Look for SAS code lines (they start with 's' followed by line number)
        if re.match(r'^s\s+\d+\s+', line):
            # Extract the SAS code part (everything after the line number)
            sas_part = re.sub(r'^s\s+\d+\s+', '', line)
            if sas_part.strip() and not sas_part.strip().startswith(';*'):
                sas_lines.append(sas_part)
    
    return '\n'.join(sas_lines)

def extract_sas_content(file_path):
    """Extract and analyze SAS code content comprehensively"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Clean content for analysis
        lines = content.split('\n')
        non_empty_lines = [line for line in lines if line.strip()]
        
        return {
            'content': content,
            'lines': lines,
            'non_empty_lines': non_empty_lines,
            'total_lines': len(lines),
            'non_blank_lines': len(non_empty_lines)
        }
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def analyze_sas_patterns(content):
    """Comprehensive pattern analysis of SAS code"""
    content_lower = content.lower()
    
    # Core SAS patterns - simplified set
    patterns = {
        'proc_sql': len(re.findall(r'\bproc\s+sql\b', content_lower)),
        'data_steps': len(re.findall(r'\bdata\s+\w+', content_lower)),
        'macros': len(re.findall(r'%macro\s+\w+', content_lower)),
        'joins': len(re.findall(r'\bjoin\b', content_lower)),
        'where_clauses': len(re.findall(r'\bwhere\b', content_lower))
    }
    
    # Database connections
    connect_stmts = re.findall(r'\bconnect\s+to\s+(\w+)', content_lower)
    
    return {
        'patterns': patterns,
        'connect_stmts': connect_stmts
    }

def extract_datasets(content):
    """Extract input and output datasets from SAS code"""
    content_lower = content.lower()
    
    # Input datasets
    inputs = []
    
    # FROM clauses
    from_matches = re.findall(r'\bfrom\s+([a-z_][a-z0-9_.]*)', content_lower)
    inputs.extend(from_matches)
    
    # JOIN clauses
    join_matches = re.findall(r'\bjoin\s+([a-z_][a-z0-9_.]*)', content_lower)
    inputs.extend(join_matches)
    
    # SET statements
    set_matches = re.findall(r'\bset\s+([a-z_][a-z0-9_.]+)', content_lower)
    inputs.extend(set_matches)
    
    # Output datasets
    outputs = []
    create_matches = re.findall(r'create\s+table\s+([a-z_][a-z0-9_.]*)', content_lower)
    outputs.extend(create_matches)
    data_matches = re.findall(r'\bdata\s+([a-z_][a-z0-9_.]+)', content_lower)
    outputs.extend(data_matches)
    
    # Clean up - remove SAS keywords and empty strings
    sas_keywords = {'work', 'sashelp', 'sasuser', '_null_', 'run', 'quit', 'end', 'then', 'else', 'do', 'if', 'where', 'by', 'and', 'or', 'select', 'from', 'join', 'on', 'as', 'when', 'case'}
    
    inputs = [inp.strip() for inp in inputs if inp.strip() and inp.strip() not in sas_keywords and len(inp.strip()) > 1]
    outputs = [out.strip() for out in outputs if out.strip() and out.strip() not in sas_keywords and len(out.strip()) > 1]
    
    return inputs, outputs

def analyze_xml_structure(xml_path):
    """Analyze the EGP XML structure for workflow information"""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Count different element types
        elements = root.findall('.//Element')
        element_types = {}
        
        for element in elements:
            type_elem = element.find('Type')
            if type_elem is not None:
                elem_type = type_elem.text
                element_types[elem_type] = element_types.get(elem_type, 0) + 1
        
        # Find project title
        project_title = "Unknown"
        first_element = root.find('.//Element')
        if first_element is not None:
            label_elem = first_element.find('Label')
            if label_elem is not None:
                project_title = label_elem.text
        
        return {
            'project_title': project_title,
            'element_types': element_types,
            'total_elements': len(elements)
        }
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None

# Main analysis workflow
print("🔎 Extracting EGP file...")
print(f"   Analyzing: {os.path.abspath(egp_file_path)}")

# Clean up extraction directory at start to ensure fresh analysis
if os.path.exists(output_dir):
    import shutil
    import stat
    try:
        # Force remove with proper error handling for Windows
        def handle_remove_readonly(func, path, exc):
            os.chmod(path, stat.S_IWRITE)
            func(path)
        
        shutil.rmtree(output_dir, onerror=handle_remove_readonly)
        print(f"   🧹 Cleared previous extraction")
    except Exception as e:
        print(f"   ⚠️  Warning: Could not fully clear previous extraction: {e}")
        print(f"   ➡️  Continuing with extraction...")

# Extract EGP file
try:
    with zipfile.ZipFile(egp_file_path, 'r') as zip_ref:
        zip_ref.extractall(output_dir)
    print("   ✅ Extraction completed")
except Exception as e:
    print(f"   ❌ Extraction failed: {e}")
    exit(1)

print("\n🔍 Scanning for files...")

# Find all files
project_xml = None
sas_files = []
log_files = []
other_files = []

for root, dirs, files in os.walk(output_dir):
    for file in files:
        full_path = os.path.join(root, file)
        if file.lower() == 'project.xml':
            project_xml = full_path
        elif file.lower().endswith('.sas'):
            sas_files.append(full_path)
        elif file.lower().endswith('.log'):
            log_files.append(full_path)
        else:
            other_files.append(full_path)

print(f"   📄 Found {len(sas_files)} SAS files")
print(f"   📄 Found {len(log_files)} log files")
print(f"   📄 Found {len(other_files)} other files")
print(f"   📄 Project XML: {'Found' if project_xml else 'Not found'}")

# Analyze XML structure
xml_info = None
if project_xml:
    print("\n📊 Analyzing project structure...")
    xml_info = analyze_xml_structure(project_xml)
    if xml_info:
        print(f"   📁 Project: {xml_info['project_title']}")
        print(f"   🔗 Total elements: {xml_info['total_elements']}")
        print("   📋 Element types:")
        for elem_type, count in xml_info['element_types'].items():
            print(f"      • {elem_type}: {count}")

# Analyze SAS files and log files
print(f"\n🗃  SAS Content Analysis:")
print("=" * 80)

all_inputs = []
all_outputs = []
all_patterns = {
    'proc_sql': 0, 'data_steps': 0, 'macros': 0,
    'joins': 0, 'where_clauses': 0, 'connect_stmts': []
}

file_details = []
sas_lines_from_files = 0
sas_lines_from_logs = 0

# Process .sas files
for sas_file in sas_files:
    rel_path = os.path.relpath(sas_file, output_dir)
    print(f"\n📄 {rel_path}")
    
    # Extract content
    sas_content = extract_sas_content(sas_file)
    if not sas_content:
        continue
    
    sas_lines_from_files += sas_content['total_lines']
    
    # Analyze patterns
    analysis = analyze_sas_patterns(sas_content['content'])
    inputs, outputs = extract_datasets(sas_content['content'])
    
    # Display file info
    print(f"   📏 Lines: {sas_content['total_lines']} total, {sas_content['non_blank_lines']} non-blank")
    
    if analysis['patterns']['data_steps'] > 0:
        print(f"   📊 DATA steps: {analysis['patterns']['data_steps']}")
    if analysis['patterns']['proc_sql'] > 0:
        print(f"   💾 PROC SQL: {analysis['patterns']['proc_sql']}")
    if len(inputs) > 0:
        print(f"   📥 Inputs: {len(inputs)} datasets")
        for inp in inputs[:5]:  # Show first 5
            print(f"      • {inp}")
        if len(inputs) > 5:
            print(f"      • ... and {len(inputs) - 5} more")
    
    if len(outputs) > 0:
        print(f"   📤 Outputs: {len(outputs)} datasets")
        for out in outputs[:3]:  # Show first 3
            print(f"      • {out}")
        if len(outputs) > 3:
            print(f"      • ... and {len(outputs) - 3} more")
    
    # Show first few lines of code for context
    print("   📝 Code preview:")
    for i, line in enumerate(sas_content['non_empty_lines'][:3]):
        print(f"      {i+1:2}: {line.strip()[:60]}{'...' if len(line.strip()) > 60 else ''}")
    
    # Aggregate data
    all_inputs.extend(inputs)
    all_outputs.extend(outputs)
    
    # Aggregate patterns
    for key in ['proc_sql', 'data_steps', 'macros', 'joins', 'where_clauses']:
        all_patterns[key] += analysis['patterns'][key]
    all_patterns['connect_stmts'].extend(analysis['connect_stmts'])

# Process log files for generated SAS code
print(f"\n📋 Log Files (Generated SAS Code):")
for log_file in log_files[:5]:  # Show first 5 log files
    rel_path = os.path.relpath(log_file, output_dir)
    print(f"\n📄 {rel_path}")
    
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            log_content = f.read()
            extracted_sas = extract_sas_from_logs(log_content)
            
            if extracted_sas:
                sas_lines_from_logs += len(extracted_sas.split('\n'))
                
                # Analyze patterns in log content
                analysis = analyze_sas_patterns(extracted_sas)
                inputs, outputs = extract_datasets(extracted_sas)
                
                print(f"   📏 Extracted SAS lines: {len(extracted_sas.split('\n'))}")
                if analysis['patterns']['proc_sql'] > 0:
                    print(f"   💾 PROC SQL: {analysis['patterns']['proc_sql']}")
                if analysis['patterns']['joins'] > 0:
                    print(f"   🔗 Joins: {analysis['patterns']['joins']}")
                
                # Aggregate data from logs
                all_inputs.extend(inputs)
                all_outputs.extend(outputs)
                
                # Aggregate patterns from logs
                for key in ['proc_sql', 'data_steps', 'macros', 'joins', 'where_clauses']:
                    all_patterns[key] += analysis['patterns'][key]
                all_patterns['connect_stmts'].extend(analysis['connect_stmts'])
    except Exception as e:
        print(f"   ❌ Error reading log: {e}")

if len(log_files) > 5:
    print(f"\n   ... and {len(log_files) - 5} more log files with generated SAS code")

# Summary Report
print(f"\n{'='*80}")
print("📊 COMPREHENSIVE ANALYSIS SUMMARY")
print(f"{'='*80}")

print(f"\n🗃  File Overview:")
print(f"   • Total SAS files: {len(sas_files)}")
print(f"   • Total log files: {len(log_files)}")
print(f"   • SAS lines from .sas files: {sas_lines_from_files}")
print(f"   • SAS lines from .log files: {sas_lines_from_logs}")
print(f"   • Total SAS lines: {sas_lines_from_files + sas_lines_from_logs}")

print(f"\n📋 SAS Code Breakdown:")
print(f"   • PROC SQL blocks: {all_patterns['proc_sql']}")
print(f"   • DATA step blocks: {all_patterns['data_steps']}")
print(f"   • Macro definitions: {all_patterns['macros']}")
print(f"   • JOIN operations: {all_patterns['joins']}")
print(f"   • WHERE clauses: {all_patterns['where_clauses']}")

if all_patterns['connect_stmts']:
    print(f"\n💾 Database Connections:")
    for db in set(all_patterns['connect_stmts']):
        print(f"   • {db.upper()}")

# Data complexity (input/output mapping)
print(f"\n📁 Data Architecture Review:")
print(f"   📥 Input datasets: {len(set(all_inputs))} unique sources")
print(f"   📤 Output datasets: {len(set(all_outputs))} deliverables")

# Show all input datasets
print(f"\n� All Input Tables:")
for i, dataset in enumerate(sorted(set(all_inputs)), 1):
    print(f"   {i:2d}. {dataset}")

# Show all output datasets  
print(f"\n� All Output Tables:")
for i, dataset in enumerate(sorted(set(all_outputs)), 1):
    print(f"   {i:2d}. {dataset}")

print(f"\n✅ Analysis completed successfully!")