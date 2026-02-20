#!/usr/bin/env python3
"""Script to clean up sede_consolidator.py by removing duplicated/broken code."""

import re

# Read the file
with open(r'c:\Users\vinicios.buzzi\buzzi\geovalida\src\pipeline\sede_consolidator.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Original file: {len(lines)} lines")

# Find where the broken code starts and correct code begins
# Looking for: line ~540 starts broken _get_total_flow
# Looking for: line ~880 has correct _get_total_flow with """Helper... docstring

broken_start = None
correct_start = None

for i, line in enumerate(lines):
    # Find broken section (after _save_results_and_csv, before correct _get_total_flow)
    if i > 535 and i < 600 and 'def _get_total_flow' in line and broken_start is None:
        broken_start = i
        print(f"Found broken _get_total_flow at line {i+1}")
    
    # Find correct section (has the docstring)
    if i > 870 and '"""Helper to get total flow' in line and correct_start is None:
        # Back up to find start of method
        for j in range(i-1, max(0, i-10), -1):
            if 'def _get_total_flow' in lines[j]:
                correct_start = j
                print(f"Found correct _get_total_flow at line {j+1}")
                break

if broken_start and correct_start:
    print(f"\nRemoving lines {broken_start+1} to {correct_start}")
    
    # Keep everything before broken section and after correct section starts
    cleaned_lines = lines[:broken_start] + lines[correct_start:]
    
    # Write cleaned file
    with open(r'c:\Users\vinicios.buzzi\buzzi\geovalida\src\pipeline\sede_consolidator.py', 'w', encoding='utf-8') as f:
        f.writelines(cleaned_lines)
    
    print(f"\nCleaned file: {len(cleaned_lines)} lines")
    print(f"Removed {len(lines) - len(cleaned_lines)} lines")
else:
    print("Could not find broken/correct sections!")
    print(f"broken_start={broken_start}, correct_start={correct_start}")
