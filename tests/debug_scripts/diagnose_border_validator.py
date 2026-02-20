"""
Diagnostic script to analyze border validation constraints and generate a detailed report.
"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

print("=" * 80)
print("BORDER VALIDATION DIAGNOSTIC REPORT")
print("=" * 80)

# Load results
result_path = Path(__file__).parent.parent.parent / "data" / "03_processed" / "border_validation_result.csv"
df = pd.read_csv(result_path)

# Analyze rejections by reason
rejections = df[df['action'] == 'REJECTED']
relocations = df[df['action'] == 'RELOCATED']

print(f"\n[SUMMARY]")
print("=" * 80)
print(f"Total relocations: {len(relocations)}")
print(f"Total rejections: {len(rejections)}")

# Group by reason
print(f"\n[REJECTION BREAKDOWN]")
print("=" * 80)
rejection_reasons = rejections.groupby('reason').size().sort_values(ascending=False)
for reason, count in rejection_reasons.items():
    print(f"{reason:30s}: {count:5d} ({count/len(rejections)*100:.1f}%)")

# Analyze RM rule violations in detail
print(f"\n[RM RULE VIOLATIONS DETAIL]")
print("=" * 80)
rm_violations = rejections[rejections['reason'] == 'RM rule violation']
print(f"Total RM violations: {len(rm_violations)}")

# Parse details to extract RM transitions
rm_transitions = {}
for _, row in rm_violations.iterrows():
    details = row['details']
    if 'Origin RM:' in details and 'Dest RM:' in details:
        parts = details.split(', ')
        origin_rm = parts[0].replace('Origin RM: ', '').strip()
        dest_rm = parts[1].replace('Dest RM: ', '').strip()
        transition = f"{origin_rm} -> {dest_rm}"
        rm_transitions[transition] = rm_transitions.get(transition, 0) + 1

print(f"\nTop 10 RM transition patterns blocked:")
for i, (transition, count) in enumerate(sorted(rm_transitions.items(), key=lambda x: x[1], reverse=True)[:10], 1):
    print(f"{i:2d}. {transition:70s}: {count:3d} cases")

# Count SEM_RM -> COM_RM transitions
sem_rm_to_com_rm = sum(1 for t, c in rm_transitions.items() if 'SEM_RM' in t.split('->')[0] and 'SEM_RM' not in t.split('->')[1])
print(f"\n[WARNING] SEM_RM -> COM_RM blocked transitions: {sem_rm_to_com_rm} ({sem_rm_to_com_rm/len(rm_violations)*100:.1f}% of RM violations)")

# Analyze flow threshold violations
print(f"\n[FLOW THRESHOLD ANALYSIS]")
print("=" * 80)
flow_weak = rejections[rejections['reason'] == 'Flow too weak']
print(f"Total flow threshold violations: {len(flow_weak)}")

# Parse percentages from details
percentages = []
for _, row in flow_weak.iterrows():
    details = row['details']
    if 'Percentage:' in details:
        pct_str = details.split('Percentage: ')[1].split('%')[0]
        try:
            percentages.append(float(pct_str))
        except:
            pass

if percentages:
    print(f"\nFlow percentage statistics:")
    print(f"  Min: {min(percentages):.1f}%")
    print(f"  Max: {max(percentages):.1f}%")
    print(f"  Mean: {sum(percentages)/len(percentages):.1f}%")
    print(f"  Median: {sorted(percentages)[len(percentages)//2]:.1f}%")
    
    # Count how many would pass with lower thresholds
    for threshold in [4.5, 4.0, 3.5, 3.0, 2.5, 2.0]:
        passing = sum(1 for p in percentages if p >= threshold)
        print(f"  Would pass with {threshold}% threshold: {passing}/{len(percentages)} ({passing/len(percentages)*100:.1f}%)")

# Analyze oscillations
print(f"\n[OSCILLATION ANALYSIS]")
print("=" * 80)
oscillations = rejections[rejections['reason'] == 'Oscillation detected']
print(f"Total oscillations: {len(oscillations)}")

if len(oscillations) > 0:
    print(f"\nMunicipalities with oscillation:")
    for _, row in oscillations.drop_duplicates('mun_id').iterrows():
        print(f"  - {row['mun_name']} ({row['mun_id']}): {row['details']}")

# Analyze iteration progression
print(f"\n[ITERATION PROGRESSION]")
print("=" * 80)

iterations = sorted(df['iteration'].unique())
for iteration in iterations:
    iter_data = df[df['iteration'] == iteration]
    iter_relocations = iter_data[iter_data['action'] == 'RELOCATED']
    iter_rejections = iter_data[iter_data['action'] == 'REJECTED']
    print(f"\nIteration {iteration}:")
    print(f"  Relocations: {len(iter_relocations)}")
    print(f"  Rejections: {len(iter_rejections)}")
    
    if len(iter_rejections) > 0:
        top_reasons = iter_rejections.groupby('reason').size().sort_values(ascending=False).head(3)
        print(f"  Top rejection reasons:")
        for reason, count in top_reasons.items():
            print(f"    - {reason}: {count}")

# Recommendations
print(f"\n[RECOMMENDATIONS]")
print("=" * 80)
print(f"1. **Relax RM Rules**: {sem_rm_to_com_rm} relocations blocked due to SEM_RM -> COM_RM")
print(f"   - Consider allowing if flow > 10% or 15%")
print(f"")
print(f"2. **Reduce Flow Threshold**: Currently 5%")
if percentages:
    threshold_3pct_gain = sum(1 for p in percentages if 3.0 <= p < 5.0)
    print(f"   - Reducing to 3% would allow {threshold_3pct_gain} more relocations")
print(f"")
print(f"3. **Handle Oscillations**: {len(oscillations.drop_duplicates('mun_id'))} municipalities oscillating")
print(f"   - Consider implementing simultaneous swap logic")
print(f"")

total_blocked = len(rm_violations) + len(flow_weak) + len(oscillations)
total_potential = len(relocations) + total_blocked
print(f"[IMPACT SUMMARY]:")
print(f"   Current: {len(relocations)} relocations")
print(f"   If rules relaxed: up to {total_potential} relocations (+{total_blocked})")

print(f"\n" + "=" * 80)
