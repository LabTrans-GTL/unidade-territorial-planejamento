import json
import sys

# Fix encoding
sys.stdout.reconfigure(encoding='utf-8')

with open('data/sede_analysis_cache.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

def get_sede_score(sede_info):
    """Calculate score (0-2) based on Airport and Tourism."""
    score = 0
    if sede_info.get('tem_aeroporto'):
        score += 1
    
    # Check tourism class
    turismo = str(sede_info.get('turismo', '')).strip()
    if "1 - Município Turístico" in turismo:
        score += 1
    
    return score

def get_regic_rank(regic_val):
    """Returns numeric rank for REGIC description."""
    if not regic_val:
        return 99
    
    r = str(regic_val).lower().strip()
    
    mapping = {
        'grande metrópole nacional': 1,
        'metrópole nacional': 2,
        'metrópole': 3,
        'capital regional a': 4,
        'capital regional b': 5,
        'capital regional c': 6,
        'centro sub-regional a': 7,
        'centro sub-regional b': 8,
        'centro de zona a': 9,
        'centro de zona b': 10,
        'centro local': 11
    }
    
    for k, v in mapping.items():
        if k in r:
            return v
    
    return 99

# Get all sede IDs
sede_map = {int(s['cd_mun_sede']): s for s in data['sede_analysis']}

# Check the 3 alerts
alerts = [s for s in data['sede_analysis'] if s.get('tem_alerta_dependencia')]

print(f"Total alerts: {len(alerts)}\n")
print("="*100)

for s in alerts:
    origem_id = int(s['cd_mun_sede'])
    destino_id = int(s.get('principal_destino_cd', 0))
    
    dest_info = sede_map.get(destino_id)
    
    if not dest_info:
        print(f"\n⚠ {s['nm_sede']} -> {s['principal_destino_nm']}: Destino não é sede!")
        continue
    
    # Calculate scores
    score_origem = get_sede_score(s)
    score_dest = get_sede_score(dest_info)
    
    print(f"\n{s['nm_sede']} ({origem_id}) -> {dest_info['nm_sede']} ({destino_id})")
    print(f"  UTP: {s['utp_id']} -> {dest_info['utp_id']}")
    print(f"\nORIGEM Score: {score_origem}")
    print(f"  - Aeroporto: {s['tem_aeroporto']}")
    print(f"  - Turismo: {s['turismo']}")
    print(f"  - REGIC: {s['regic']} (rank: {get_regic_rank(s['regic'])})")
    
    print(f"\nDESTINO Score: {score_dest}")
    print(f"  - Aeroporto: {dest_info['tem_aeroporto']}")
    print(f"  - Turismo: {dest_info['turismo']}")
    print(f"  - REGIC: {dest_info['regic']} (rank: {get_regic_rank(dest_info['regic'])})")
    
    print(f"\nFLUXO:")
    print(f"  - Tempo: {s['tempo_ate_destino_h']:.2f}h")
    print(f"  - Proporção: {s['proporcao_fluxo_principal']:.2%}")
    
    # Apply consolidation rules
    print(f"\nCONSOLIDATION LOGIC:")
    
    is_candidate = False
    reason = ""
    
    if score_origem == 0:
        if score_dest == 0:
            # Case 0/0: REGIC-based
            rank_orig = get_regic_rank(s['regic'])
            rank_dest = get_regic_rank(dest_info['regic'])
            
            if rank_dest < rank_orig:
                is_candidate = True
                reason = f"[OK] 0/0: REGIC Priority ({dest_info['regic']} > {s['regic']})"
            else:
                reason = f"[REJECT] 0/0: Target REGIC ({dest_info['regic']}, rank {rank_dest}) NOT > Origin ({s['regic']}, rank {rank_orig})"
        
        elif score_dest in [1, 2]:
            # Case 0/1, 0/2: Airport-based
            if dest_info['tem_aeroporto']:
                is_candidate = True
                reason = f"[OK] 0/{score_dest}: Target has Airport"
            else:
                reason = f"[REJECT] 0/{score_dest}: Target lacks Airport"
        else:
            reason = f"[REJECT]: Target Score {score_dest} invalid"
    else:
        reason = f"[REJECT]: Origin Score {score_origem} != 0"
    
    print(f"  {reason}")
    print(f"  IS CANDIDATE: {is_candidate}")
    print("="*100)
