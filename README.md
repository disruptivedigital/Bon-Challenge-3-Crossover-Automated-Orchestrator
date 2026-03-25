# Challenge #3: Crossover — Automated Orchestrator

`bon-blaster-ch3-auto.py` — fully automated MultiversX Battle of Nodes Challenge #3 script.

---

## Architecture

The script is a single-file autonomous orchestrator built around a `CrossoverEngine` class that handles all network I/O, wallet management, and transaction logic. It reuses the same patterns from the Challenge #4 reference scripts: HTTP session with ordered endpoint fallback, bulk `/transaction/send-multiple` broadcasting, concurrent chunk sending, and wave-based nonce gap management.

**Cross-shard enforcement**: On wallet load, a `cross_shard_receivers` map is precomputed per shard — for each sender shard, the receiver list contains only addresses from the *other two* shards. This makes intra-shard sends structurally impossible.

---

## Execution Flow

1. **Startup** — Loads leader + Part 1 + Part 2 wallets, validates zero overlap, verifies cross-shard is possible (wallets span >= 2 shards)
2. **Funding detection** — Sleeps until `FUNDING_CHECK_START`, then polls leader wallet balance every 15s until >= 2500 EGLD detected
3. **Fund Part 1** — Distributes 1000 EGLD across 250 local P1 wallets from leader (wave-based, skips already-funded wallets for resumability)
4. **Part 1 blast (16:00–16:30 UTC)** — Continuous cross-shard tx waves at 1e-18 EGLD value, auto-stops at window end
5. **Fund Part 2** — During the 16:30–17:00 break, distributes 250 EGLD across 250 P2 wallets
6. **Part 2 blast (17:00–17:30 UTC)** — Cross-shard txs at 0.01 EGLD value
7. **Summary** — Prints final stats

---

## Budget Split

| | Full Guild | This Server (local share) |
|---|---|---|
| Part 1 wallets | 500 | 250 |
| Part 1 budget | 2000 EGLD | 1000 EGLD |
| Part 2 wallets | 500 | 250 |
| Part 2 budget | 500 EGLD | 250 EGLD |
| **Total funding** | **2500 EGLD** | — |

The other server runs the same script independently with its own 250+250 wallets.

---

## Time Windows (UTC)

| Event | Time (UTC) |
|---|---|
| Funding check starts | 15:00 |
| Part 1 start | 16:00 |
| Part 1 end | 16:30 |
| Fund Part 2 (break) | 16:30–17:00 |
| Part 2 start | 17:00 |
| Part 2 end | 17:30 |

---

## Assumptions

- **Wallet file format**: `.pem` files named `{bech32_address}.pem` (same as Challenge #4)
- **3 shards** (0, 1, 2) — standard MultiversX shard layout
- **Leader wallet**: single `.pem` file in `leader_wallet/` directory
- **Challenge dates**: Placeholder `2026-03-25` in `.env` — update before running
- **Gas**: 50000 gas limit, 1B gas price = 0.00005 EGLD per tx (standard plain EGLD transfer)
- **The other server** runs the same script independently with its own separate 250+250 wallet set

---

## Manual Prerequisites

### 1. Install dependencies

```bash
python3 -m venv ./venv
source ./venv/bin/activate
pip install -r requirements.txt
```

### 2. Directory structure

Place files relative to the script location (`orchestrator/`):

```
orchestrator/
├── bon-blaster-ch3-auto.py
├── .env
├── leader_wallet/
│   └── erd1...leader.pem
├── wallets_part_1/
│   ├── erd1...wallet001.pem
│   └── ... (250 wallets)
└── wallets_part_2/
    ├── erd1...wallet251.pem
    └── ... (250 wallets)
```

> **Important**: Part 1 and Part 2 wallets must be completely different sets. The script validates this on startup and exits if any overlap is detected.

### 3. Update `.env`

Set the correct challenge date and confirm proxy URLs:

```env
FUNDING_CHECK_START=YYYY-MM-DD 15:00
PART1_START=YYYY-MM-DD 16:00
PART1_END=YYYY-MM-DD 16:30
PART2_START=YYYY-MM-DD 17:00
PART2_END=YYYY-MM-DD 17:30
```

### 4. Run

```bash
python bon-blaster-ch3-auto.py
```

Select primary proxy when prompted, then the script runs fully automatically.

---

## Key Script Behaviours

- **Resumable funding**: wallets already holding >= 50% of the target amount are skipped — safe to restart after interruption
- **Graceful shutdown**: `Ctrl+C` / `SIGTERM` stops after the current wave completes
- **No intra-shard txs**: receiver selection is structurally limited to cross-shard addresses only
- **Leader never sends challenge txs**: leader is used exclusively for funding distributions
- **Duplicate-fund guard**: tracks funded addresses in memory; re-checks on-chain balance before each funding wave
- **Endpoint fallback**: all network calls try proxies in order (SO01 → SO02 → gateway → API)
