import zipfile
import os
import re
import xml.etree.ElementTree as ET
import csv
import glob
import shutil

# === CONFIGURATION ===
egp_files_dir = "egp_files"
output_csv = "egp_comprehensive_analysis.csv"

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

def count_sas_patterns(content):
    """Count SAS patterns in code"""
    if not content:
        return {
            'proc_sql': 0,
            'data_steps': 0,
            'macros': 0,
            'joins': 0,
            'where': 0
        }
    
    content_lower = content.lower()
    
    return {
        'proc_sql': len(re.findall(r'\bproc\s+sql\b', content_lower)),
        'data_steps': len(re.findall(r'\bdata\s+\w+', content_lower)),
        'macros': len(re.findall(r'%macro\s+\w+', content_lower)),
        'joins': len(re.findall(r'\bjoin\b', content_lower)),
        'where': len(re.findall(r'\bwhere\b', content_lower))
    }

def get_datasets(content):
    """Extract dataset names from SAS code"""
    if not content:
        return [], []
    
    content_lower = content.lower()
    
    # Input datasets
    inputs = []
    inputs.extend(re.findall(r'\bfrom\s+(\w+(?:\.\w+)?)', content_lower))
    inputs.extend(re.findall(r'\bset\s+(\w+(?:\.\w+)?)', content_lower))
    inputs.extend(re.findall(r'\bjoin\s+(\w+(?:\.\w+)?)', content_lower))
    inputs.extend(re.findall(r'\bmerge\s+(\w+(?:\.\w+)?)', content_lower))
    
    # Output datasets
    outputs = []
    outputs.extend(re.findall(r'create\s+table\s+(\w+(?:\.\w+)?)', content_lower))
    outputs.extend(re.findall(r'\bdata\s+(\w+(?:\.\w+)?)', content_lower))
    
    # Clean up
    inputs = [x for x in set(inputs) if len(x) > 2 and x not in ['run', 'quit', 'end', 'work']]
    outputs = [x for x in set(outputs) if len(x) > 2 and x not in ['run', 'quit', 'end']]
    
    return inputs, outputs

def analyze_egp_comprehensive(egp_path):
    """Comprehensive EGP analysis including log files"""
    filename = os.path.basename(egp_path)
    print(f"Analyzing: {filename}")
    
    # Extract to temp directory
    temp_dir = f"temp_{filename.replace('.egp', '').replace(' ', '_').replace('-', '_')}"
    
    try:
        with zipfile.ZipFile(egp_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except Exception as e:
        print(f"  Error extracting: {e}")
        return None
    
    # Find ALL files
    sas_files = []
    log_files = []
    all_files = []
    
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
            
            if file.lower().endswith('.sas'):
                sas_files.append(full_path)
            elif file.lower().endswith('.log'):
                log_files.append(full_path)
    
    print(f"  Total files: {len(all_files)}")
    print(f"  SAS files: {len(sas_files)}")
    print(f"  Log files: {len(log_files)}")
    
    # Read all SAS content from .sas files
    sas_content = ""
    sas_lines = 0
    
    for sas_file in sas_files:
        try:
            with open(sas_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                sas_content += content + "\n"
                sas_lines += len(content.split('\n'))
        except Exception as e:
            print(f"    Error reading {sas_file}: {e}")
    
    # Read and extract SAS code from log files
    log_sas_content = ""
    log_sas_lines = 0
    
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                log_content = f.read()
                extracted_sas = extract_sas_from_logs(log_content)
                if extracted_sas:
                    log_sas_content += extracted_sas + "\n"
                    log_sas_lines += len(extracted_sas.split('\n'))
        except Exception as e:
            print(f"    Error reading {log_file}: {e}")
    
    # Combine all SAS content
    all_sas_content = sas_content + "\n" + log_sas_content
    total_lines = sas_lines + log_sas_lines
    
    print(f"  SAS lines from .sas files: {sas_lines}")
    print(f"  SAS lines from .log files: {log_sas_lines}")
    print(f"  Total SAS lines: {total_lines}")
    
    # Get project name from XML
    project_name = filename.replace('.egp', '')
    try:
        project_xml = os.path.join(temp_dir, 'project.xml')
        if os.path.exists(project_xml):
            tree = ET.parse(project_xml)
            root = tree.getroot()
            first_element = root.find('.')
            if first_element is not None:
                label_elem = first_element.find('Label')
                if label_elem is not None:
                    project_name = label_elem.text
    except:
        pass
    
    # Analyze patterns
    patterns = count_sas_patterns(all_sas_content)
    inputs, outputs = get_datasets(all_sas_content)
    
    # Cleanup temp directory
    try:
        shutil.rmtree(temp_dir)
    except:
        pass
    
    return {
        'filename': filename,
        'project_name': project_name,
        'total_files': len(all_files),
        'sas_files': len(sas_files),
        'log_files': len(log_files),
        'sas_lines_from_files': sas_lines,
        'sas_lines_from_logs': log_sas_lines,
        'total_sas_lines': total_lines,
        'proc_sql': patterns['proc_sql'],
        'data_steps': patterns['data_steps'],
        'macros': patterns['macros'],
        'joins': patterns['joins'],
        'where_clauses': patterns['where'],
        'input_datasets': len(inputs),
        'output_datasets': len(outputs),
        'input_list': '; '.join(inputs[:10]),
        'output_list': '; '.join(outputs[:10]),
        'has_sas_code': 'Yes' if total_lines > 0 else 'No',
        'project_type': 'Code-heavy' if sas_lines > log_sas_lines else 'Query-based' if log_sas_lines > 0 else 'Visual-only'
    }

def main():
    print("🔍 Comprehensive EGP Analysis (including log files)")
    print("=" * 60)
    
    # Find all EGP files
    if not os.path.exists(egp_files_dir):
        print(f"❌ Directory {egp_files_dir} not found!")
        return
    
    egp_files = glob.glob(os.path.join(egp_files_dir, "*.egp"))
    
    if not egp_files:
        print(f"❌ No EGP files found in {egp_files_dir}")
        return
    
    print(f"Found {len(egp_files)} EGP files")
    
    results = []
    total_sas_files = 0
    total_log_files = 0
    total_sas_lines = 0
    files_with_code = 0
    
    for egp_file in egp_files:
        result = analyze_egp_comprehensive(egp_file)
        if result:
            results.append(result)
            total_sas_files += result['sas_files']
            total_log_files += result['log_files'] 
            total_sas_lines += result['total_sas_lines']
            if result['total_sas_lines'] > 0:
                files_with_code += 1
    
    # Write results to CSV
    if results:
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n✅ Results saved to: {output_csv}")
        print(f"📊 Summary:")
        print(f"   Files with SAS code: {files_with_code}")
        print(f"   Files without SAS code: {len(results) - files_with_code}")
        print(f"   Total .sas files: {total_sas_files}")
        print(f"   Total .log files: {total_log_files}")
        print(f"   Total SAS lines: {total_sas_lines}")

if __name__ == "__main__":
    main()