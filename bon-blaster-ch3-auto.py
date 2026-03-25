#!/usr/bin/env python3
"""
Battle of Nodes — Challenge #3: Crossover — Fully Automated Orchestrator

This script runs autonomously on ONE of the two servers.
Each server manages 250 wallets for Part 1 and 250 wallets for Part 2.

Flow:
  1. Poll leader wallet for incoming challenge funds (2500 EGLD total guild).
  2. Fund 250 Part 1 wallets from leader (1000 EGLD local share).
  3. At Part 1 window (16:00–16:30 UTC): blast cross-shard MoveBalance txs.
  4. During break (16:30–17:00 UTC): fund 250 Part 2 wallets (250 EGLD local share).
  5. At Part 2 window (17:00–17:30 UTC): blast cross-shard MoveBalance txs.
  6. Stop all activity at 17:30 UTC.

Only cross-shard transactions count. Intra-shard sends are strictly prevented.
"""

import concurrent.futures
import os
import random
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv
from multiversx_sdk import Account, TransactionComputer
from multiversx_sdk.core import Transaction
from multiversx_sdk.core.address import Address, AddressComputer

# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════
load_dotenv(Path(__file__).parent / ".env")

# Network endpoints
PROXY_SO01_URL = os.getenv("PROXY_SO01_URL", "")
PROXY_SO02_URL = os.getenv("PROXY_SO02_URL", "")
BON_API_URL = os.getenv("BON_API_URL", "https://api.battleofnodes.com")
BON_GATEWAY_URL = os.getenv("BON_GATEWAY_URL", "https://gateway.battleofnodes.com")

# Chain
CHAIN_ID = os.getenv("CHAIN_ID", "B")
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "50000"))
GAS_PRICE = int(os.getenv("GAS_PRICE", "1000000000"))
GAS_COST_PER_TX = GAS_LIMIT * GAS_PRICE  # 50000000000000 = 0.00005 EGLD

# TX values: Part 1 = 1e-18 EGLD (1 atomic unit), Part 2 = 0.01 EGLD
TX_VALUE_PART1 = int(os.getenv("TX_VALUE_PART1", "1"))
TX_VALUE_PART2 = int(os.getenv("TX_VALUE_PART2", str(10**16)))  # 0.01 EGLD

# Batching / throughput
BULK_TX_LIMIT = int(os.getenv("BULK_TX_LIMIT", "50000"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))
CONCURRENT_CHUNKS = int(os.getenv("CONCURRENT_CHUNKS", "3"))
MAX_NONCE_GAP = int(os.getenv("MAX_NONCE_GAP", "80"))
SLEEP_BETWEEN_WAVES = int(os.getenv("SLEEP_BETWEEN_WAVES", "6"))
BALANCE_CHECK_INTERVAL = int(os.getenv("BALANCE_CHECK_INTERVAL", "10"))

# Wallet paths (relative to script dir or absolute)
LEADER_WALLET_DIR = os.getenv("LEADER_WALLET_DIR", "./leader_wallet")
WALLETS_PART1_DIR = os.getenv("WALLETS_PART1_DIR", "./wallets_part_1")
WALLETS_PART2_DIR = os.getenv("WALLETS_PART2_DIR", "./wallets_part_2")

# Budget (this server's local share)
LOCAL_BUDGET_PART1_EGLD = float(os.getenv("LOCAL_BUDGET_PART1_EGLD", "1000"))
LOCAL_BUDGET_PART2_EGLD = float(os.getenv("LOCAL_BUDGET_PART2_EGLD", "250"))
LOCAL_WALLETS_PER_PART = int(os.getenv("LOCAL_WALLETS_PER_PART", "250"))

# Funding detection
EXPECTED_FUNDING_EGLD = float(os.getenv("EXPECTED_FUNDING_EGLD", "2500"))
FUNDING_CHECK_START = os.getenv("FUNDING_CHECK_START", "2026-03-25 15:00")  # UTC
FUNDING_POLL_INTERVAL = int(os.getenv("FUNDING_POLL_INTERVAL", "15"))  # seconds

# Time windows (UTC) — format: "YYYY-MM-DD HH:MM"
PART1_START = os.getenv("PART1_START", "2026-03-25 16:00")
PART1_END = os.getenv("PART1_END", "2026-03-25 16:30")
PART2_START = os.getenv("PART2_START", "2026-03-25 17:00")
PART2_END = os.getenv("PART2_END", "2026-03-25 17:30")

# Retry
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))

# ═══════════════════════════════════════════════════════════════════════════════
#  GLOBALS
# ═══════════════════════════════════════════════════════════════════════════════
SHUTDOWN = threading.Event()
ENDPOINT_ORDER: list[str] = []


def _parse_utc(s: str) -> datetime:
    return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: datetime = None) -> str:
    return (dt or _now_utc()).strftime("%H:%M:%S")


# ═══════════════════════════════════════════════════════════════════════════════
#  LOGGING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{_ts()}] {msg}", flush=True)


def log_ok(msg: str):
    print(f"[{_ts()}] [OK] {msg}", flush=True)


def log_warn(msg: str):
    print(f"[{_ts()}] [WARN] {msg}", flush=True)


def log_err(msg: str):
    print(f"[{_ts()}] [ERROR] {msg}", flush=True)


def header(title: str):
    print(f"\n{'=' * 70}", flush=True)
    print(f"  {title}  ({_ts()} UTC)", flush=True)
    print(f"{'=' * 70}", flush=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  PROXY SELECTION
# ═══════════════════════════════════════════════════════════════════════════════
def build_endpoint_order() -> list[str]:
    proxies = []
    if PROXY_SO01_URL:
        proxies.append(("SO01", PROXY_SO01_URL))
    if PROXY_SO02_URL:
        proxies.append(("SO02", PROXY_SO02_URL))

    if not proxies:
        endpoints = [url for url in [BON_GATEWAY_URL, BON_API_URL] if url]
        log_warn(f"No PROXY_SO*_URL set — using public endpoints only: {', '.join(endpoints)}")
        return endpoints

    print("\n" + "=" * 60)
    print("  SELECT PRIMARY PROXY")
    print("=" * 60)
    for i, (label, url) in enumerate(proxies, 1):
        print(f"  {i}. {label}: {url}")
    print("=" * 60)

    try:
        choice = input(f"\nSelect primary proxy [1-{len(proxies)}]: ").strip()
        idx = int(choice) - 1
        if 0 <= idx < len(proxies):
            primary_label, primary_url = proxies[idx]
            backup = [url for j, (_, url) in enumerate(proxies) if j != idx]
            ordered = [primary_url] + backup + [BON_GATEWAY_URL, BON_API_URL]
            log_ok(f"Primary: {primary_label} ({primary_url})")
            return ordered
    except KeyboardInterrupt:
        print("\nAborted.")
        os._exit(0)
    except ValueError:
        pass

    primary_url = proxies[0][1]
    backup = [url for j, (_, url) in enumerate(proxies) if j != 0]
    return [primary_url] + backup + [BON_GATEWAY_URL, BON_API_URL]


# ═══════════════════════════════════════════════════════════════════════════════
#  CROSSOVER ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
class CrossoverEngine:
    """Core engine for Challenge #3 cross-shard transaction automation."""

    def __init__(self):
        self.address_computer = AddressComputer()
        self.tx_computer = TransactionComputer()
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._chain_id_cache = None

        # Wallets grouped by shard {shard_id: [Account, ...]}
        self.shard_wallets_p1: dict[int, list[Account]] = {0: [], 1: [], 2: []}
        self.shard_wallets_p2: dict[int, list[Account]] = {0: [], 1: [], 2: []}
        self.all_wallets_p1: list[Account] = []
        self.all_wallets_p2: list[Account] = []

        # Cross-shard receiver maps: {sender_shard: [receiver_addresses_in_other_shards]}
        self.cross_shard_receivers_p1: dict[int, list[Address]] = {}
        self.cross_shard_receivers_p2: dict[int, list[Address]] = {}

        # Leader wallet
        self.leader: Account = None

        # Stats
        self.stats = {
            "p1_sent": 0, "p1_accepted": 0, "p1_failed": 0,
            "p2_sent": 0, "p2_accepted": 0, "p2_failed": 0,
            "funding_p1_sent": 0, "funding_p2_sent": 0,
        }
        self._funded_addresses: set[str] = set()

    # ── Network helpers ─────────────────────────────────────────────────────

    def _get_chain_id(self) -> str:
        if self._chain_id_cache:
            return self._chain_id_cache
        for url in ENDPOINT_ORDER:
            try:
                r = self.session.get(f"{url}/network/config", timeout=10)
                self._chain_id_cache = r.json()["data"]["config"]["erd_chain_id"]
                return self._chain_id_cache
            except Exception:
                continue
        return CHAIN_ID

    def _get_account_info(self, address: str) -> tuple[int, int]:
        """Returns (nonce, balance) in atomic units."""
        for url in ENDPOINT_ORDER:
            try:
                r = self.session.get(f"{url}/address/{address}", timeout=5)
                data = r.json()["data"]["account"]
                return data["nonce"], int(data["balance"])
            except Exception:
                continue
        return 0, 0

    def _get_balance(self, address: str) -> int:
        _, balance = self._get_account_info(address)
        return balance

    def _get_nonce(self, address: str) -> int:
        nonce, _ = self._get_account_info(address)
        return nonce

    def _send_bulk(self, txs_json: list[dict]) -> tuple[int, list]:
        if not txs_json:
            return 0, []
        for url in ENDPOINT_ORDER:
            if not url:
                continue
            try:
                r = self.session.post(
                    f"{url}/transaction/send-multiple",
                    json=txs_json,
                    timeout=120,
                )
                data = r.json()
                if "data" in data:
                    num_sent = data["data"].get("numOfSentTxs", 0)
                    hashes = data["data"].get("txsHashes", {})
                    return num_sent, list(hashes.values()) if hashes else []
                return 0, []
            except Exception as e:
                log_warn(f"Bulk send error ({url}): {e}")
                continue
        return 0, []

    # ── Wallet loading ──────────────────────────────────────────────────────

    def load_leader(self):
        leader_dir = Path(LEADER_WALLET_DIR)
        if not leader_dir.exists():
            log_err(f"Leader wallet dir not found: {leader_dir.resolve()}")
            sys.exit(1)

        pem_files = list(leader_dir.glob("*.pem"))
        if not pem_files:
            log_err(f"No .pem files in {leader_dir.resolve()}")
            sys.exit(1)

        self.leader = Account.new_from_pem(pem_files[0])
        balance = self._get_balance(self.leader.address.to_bech32())
        log(f"Leader: {self.leader.address.to_bech32()}")
        log(f"Leader balance: {balance / 10**18:.4f} EGLD")

    def _load_wallets_from_dir(self, wallet_dir: str) -> tuple[list[Account], dict[int, list[Account]]]:
        d = Path(wallet_dir)
        if not d.exists():
            log_err(f"Wallet dir not found: {d.resolve()}")
            sys.exit(1)

        pem_files = sorted(d.glob("*.pem"))
        if not pem_files:
            log_err(f"No .pem files in {d.resolve()}")
            sys.exit(1)

        all_wallets = []
        shard_wallets: dict[int, list[Account]] = {0: [], 1: [], 2: []}

        for pem_file in pem_files:
            try:
                account = Account.new_from_pem(pem_file)
                shard = self.address_computer.get_shard_of_address(account.address)
                shard_wallets[shard].append(account)
                all_wallets.append(account)
            except Exception as e:
                log_warn(f"Failed to load {pem_file.name}: {e}")

        return all_wallets, shard_wallets

    def load_part1_wallets(self):
        self.all_wallets_p1, self.shard_wallets_p1 = self._load_wallets_from_dir(WALLETS_PART1_DIR)
        self._build_cross_shard_map(self.shard_wallets_p1, self.cross_shard_receivers_p1)
        self._print_shard_summary("Part 1", self.shard_wallets_p1)

    def load_part2_wallets(self):
        self.all_wallets_p2, self.shard_wallets_p2 = self._load_wallets_from_dir(WALLETS_PART2_DIR)
        self._build_cross_shard_map(self.shard_wallets_p2, self.cross_shard_receivers_p2)
        self._print_shard_summary("Part 2", self.shard_wallets_p2)

    def _build_cross_shard_map(self, shard_wallets: dict, receiver_map: dict):
        """For each shard, build a list of receiver addresses from OTHER shards."""
        for sender_shard in [0, 1, 2]:
            receivers = []
            for recv_shard in [0, 1, 2]:
                if recv_shard == sender_shard:
                    continue
                receivers.extend([w.address for w in shard_wallets[recv_shard]])
            receiver_map[sender_shard] = receivers
        log(f"Cross-shard maps built: " +
            ", ".join(f"S{s}->{len(receiver_map[s])} receivers" for s in [0, 1, 2]))

    def _print_shard_summary(self, label: str, shard_wallets: dict):
        total = sum(len(w) for w in shard_wallets.values())
        log(f"{label}: {total} wallets loaded — " +
            ", ".join(f"S{s}:{len(shard_wallets[s])}" for s in [0, 1, 2]))

    # ── Validation ──────────────────────────────────────────────────────────

    def validate_no_overlap(self):
        """Ensure Part 1 and Part 2 wallet sets have zero overlap."""
        p1_addrs = {w.address.to_bech32() for w in self.all_wallets_p1}
        p2_addrs = {w.address.to_bech32() for w in self.all_wallets_p2}
        overlap = p1_addrs & p2_addrs
        if overlap:
            log_err(f"FATAL: {len(overlap)} wallets shared between Part 1 and Part 2!")
            for addr in list(overlap)[:5]:
                log_err(f"  Overlap: {addr}")
            sys.exit(1)
        log_ok(f"Validation passed: 0 wallet overlap between Part 1 ({len(p1_addrs)}) and Part 2 ({len(p2_addrs)})")

    def validate_cross_shard_possible(self, shard_wallets: dict, label: str):
        """Ensure at least 2 shards have wallets so cross-shard is possible."""
        populated = [s for s in [0, 1, 2] if shard_wallets[s]]
        if len(populated) < 2:
            log_err(f"FATAL: {label} wallets only in {len(populated)} shard(s) — cross-shard impossible!")
            sys.exit(1)

    # ── Funding detection ───────────────────────────────────────────────────

    def wait_for_funding(self):
        """Poll leader wallet until expected funds arrive."""
        header("WAITING FOR CHALLENGE FUNDING")
        expected_atomic = int(EXPECTED_FUNDING_EGLD * 10**18)
        check_start = _parse_utc(FUNDING_CHECK_START)

        # Wait until funding check window
        now = _now_utc()
        if now < check_start:
            wait_secs = (check_start - now).total_seconds()
            wait_str = str(timedelta(seconds=int(wait_secs)))
            log(f"Funding check starts at {FUNDING_CHECK_START} UTC — waiting {wait_str}...")
            while _now_utc() < check_start and not SHUTDOWN.is_set():
                time.sleep(min(30, (check_start - _now_utc()).total_seconds()))

        log(f"Polling leader wallet for >= {EXPECTED_FUNDING_EGLD} EGLD...")
        while not SHUTDOWN.is_set():
            balance = self._get_balance(self.leader.address.to_bech32())
            log(f"Leader balance: {balance / 10**18:.4f} EGLD (need {EXPECTED_FUNDING_EGLD})")
            if balance >= expected_atomic:
                log_ok(f"Challenge funds detected! Balance: {balance / 10**18:.4f} EGLD")
                return True
            time.sleep(FUNDING_POLL_INTERVAL)

        return False

    # ── Funding wallets ─────────────────────────────────────────────────────

    def fund_wallets(self, wallets: list[Account], total_budget_egld: float, label: str) -> int:
        """Fund wallets from leader. Returns number successfully funded."""
        header(f"FUNDING {label}")

        total_budget = int(total_budget_egld * 10**18)
        amount_per_wallet = total_budget // len(wallets)

        # Check which wallets already have funds (resumability)
        wallets_to_fund = []
        already_funded = 0
        total_existing_balance = 0
        min_funded_threshold = amount_per_wallet // 2  # consider funded if >= 50% of target

        for w in wallets:
            addr = w.address.to_bech32()
            if addr in self._funded_addresses:
                already_funded += 1
                continue
            balance = self._get_balance(addr)
            total_existing_balance += balance
            if balance >= min_funded_threshold:
                self._funded_addresses.add(addr)
                already_funded += 1
                continue
            wallets_to_fund.append(w)

        if already_funded > 0:
            log(f"Already funded: {already_funded}/{len(wallets)} — skipping")
            log(f"Total balance across wallets: {total_existing_balance / 10**18:.4f} EGLD")

        if not wallets_to_fund:
            log_ok(f"All {len(wallets)} wallets already funded!")
            return len(wallets)

        log(f"Funding {len(wallets_to_fund)} wallets @ {amount_per_wallet / 10**18:.6f} EGLD each")
        log(f"Total to distribute: {amount_per_wallet * len(wallets_to_fund) / 10**18:.4f} EGLD")

        # Check leader has enough
        leader_balance = self._get_balance(self.leader.address.to_bech32())
        needed = amount_per_wallet * len(wallets_to_fund) + GAS_COST_PER_TX * len(wallets_to_fund)
        if leader_balance < needed:
            log_err(f"Leader balance {leader_balance / 10**18:.4f} < needed {needed / 10**18:.4f} EGLD")
            # Fund as many as possible
            affordable = (leader_balance - GAS_COST_PER_TX) // (amount_per_wallet + GAS_COST_PER_TX)
            if affordable <= 0:
                log_err("Cannot fund any wallets!")
                return already_funded
            log_warn(f"Can only fund {affordable} wallets")
            wallets_to_fund = wallets_to_fund[:affordable]

        chain_id = self._get_chain_id()
        total_funded = already_funded
        wave_size = MAX_NONCE_GAP
        remaining = wallets_to_fund.copy()
        wave_num = 0

        while remaining and not SHUTDOWN.is_set():
            wave_num += 1
            wave = remaining[:wave_size]
            remaining = remaining[wave_size:]

            # Sync leader nonce
            self.leader.nonce = self._get_nonce(self.leader.address.to_bech32())

            # Build funding transactions
            txs_json = []
            for w in wave:
                tx = Transaction(
                    sender=self.leader.address,
                    receiver=w.address,
                    value=amount_per_wallet,
                    gas_limit=GAS_LIMIT,
                    gas_price=GAS_PRICE,
                    chain_id=chain_id,
                    nonce=self.leader.get_nonce_then_increment(),
                    version=1,
                )
                tx.signature = self.leader.sign(self.tx_computer.compute_bytes_for_signing(tx))
                txs_json.append({
                    "nonce": tx.nonce,
                    "value": str(tx.value),
                    "receiver": tx.receiver.to_bech32(),
                    "sender": tx.sender.to_bech32(),
                    "gasPrice": tx.gas_price,
                    "gasLimit": tx.gas_limit,
                    "signature": tx.signature.hex(),
                    "chainID": tx.chain_id,
                    "version": tx.version,
                })

            # Send in chunks
            sent = self._broadcast_chunks(f"FUND-{label}-W{wave_num}", txs_json)
            total_funded += sent

            for w in wave[:sent]:
                self._funded_addresses.add(w.address.to_bech32())

            log(f"Wave {wave_num}: {sent}/{len(wave)} funded | Total: {total_funded}/{len(wallets)}")

            if remaining:
                time.sleep(SLEEP_BETWEEN_WAVES)

        stat_key = "funding_p1_sent" if "P1" in label else "funding_p2_sent"
        self.stats[stat_key] = total_funded - already_funded
        log_ok(f"{label} funding complete: {total_funded}/{len(wallets)} wallets funded")
        return total_funded

    # ── Transaction generation (cross-shard only) ───────────────────────────

    def _create_tx_json(self, sender: Account, receiver_addr: Address,
                        chain_id: str, value: int) -> dict:
        tx = Transaction(
            sender=sender.address,
            receiver=receiver_addr,
            value=value,
            gas_limit=GAS_LIMIT,
            gas_price=GAS_PRICE,
            chain_id=chain_id,
            nonce=sender.get_nonce_then_increment(),
            version=1,
        )
        tx.signature = sender.sign(self.tx_computer.compute_bytes_for_signing(tx))
        return {
            "nonce": tx.nonce,
            "value": str(tx.value),
            "receiver": tx.receiver.to_bech32(),
            "sender": tx.sender.to_bech32(),
            "gasPrice": tx.gas_price,
            "gasLimit": tx.gas_limit,
            "signature": tx.signature.hex(),
            "chainID": tx.chain_id,
            "version": tx.version,
        }

    def generate_cross_shard_txs(self, shard_wallets: dict[int, list[Account]],
                                  receiver_map: dict[int, list[Address]],
                                  txs_per_wallet: int, value: int) -> list[dict]:
        """Generate cross-shard txs for all shards. Returns list of tx JSON dicts."""
        chain_id = self._get_chain_id()
        all_txs = []

        for sender_shard in [0, 1, 2]:
            wallets = shard_wallets[sender_shard]
            receivers = receiver_map.get(sender_shard, [])
            if not wallets or not receivers:
                continue

            # Sync nonces for this shard
            for w in wallets:
                w.nonce = self._get_nonce(w.address.to_bech32())

            for _ in range(txs_per_wallet):
                for sender in wallets:
                    receiver = random.choice(receivers)
                    all_txs.append(self._create_tx_json(sender, receiver, chain_id, value))

        return all_txs

    # ── Broadcasting ────────────────────────────────────────────────────────

    def _broadcast_chunks(self, label: str, txs: list[dict]) -> int:
        """Broadcast tx list in concurrent chunks. Returns total accepted."""
        if not txs:
            return 0

        total_sent = 0
        remaining = txs.copy()
        consecutive_zero = 0

        while remaining and not SHUTDOWN.is_set():
            chunks = []
            for _ in range(CONCURRENT_CHUNKS):
                if remaining:
                    chunk = remaining[:CHUNK_SIZE]
                    remaining = remaining[CHUNK_SIZE:]
                    chunks.append(chunk)

            if not chunks:
                break

            batch_sent = 0

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(chunks)) as ex:
                futures = [ex.submit(self._send_bulk, chunk) for chunk in chunks]
                for future in concurrent.futures.as_completed(futures):
                    sent, _ = future.result()
                    batch_sent += sent

            total_sent += batch_sent

            if batch_sent == 0:
                consecutive_zero += 1
            else:
                consecutive_zero = 0

            if consecutive_zero >= 2:
                dropped = len(remaining)
                if dropped:
                    log_warn(f"[{label}] Mempool full — dropping {dropped} remaining txs")
                break

        return total_sent

    # ── Blast phase ─────────────────────────────────────────────────────────

    def run_blast_phase(self, part: int, shard_wallets: dict, receiver_map: dict,
                        tx_value: int, start_dt: datetime, end_dt: datetime):
        """Run continuous cross-shard blasting within a time window."""
        part_label = f"Part {part}"
        header(f"{part_label} — CROSS-SHARD BLAST")

        # Wait for start time
        now = _now_utc()
        if now < start_dt:
            wait_secs = (start_dt - now).total_seconds()
            wait_str = str(timedelta(seconds=int(wait_secs)))
            log(f"Waiting {wait_str} for {part_label} start ({start_dt.strftime('%Y-%m-%d %H:%M')} UTC)...")
            while _now_utc() < start_dt and not SHUTDOWN.is_set():
                remaining = (start_dt - _now_utc()).total_seconds()
                time.sleep(min(10, max(0.1, remaining)))

        if SHUTDOWN.is_set():
            return

        log_ok(f"{part_label} STARTED — blasting until {end_dt.strftime('%Y-%m-%d %H:%M')} UTC")

        total_wallets = sum(len(w) for w in shard_wallets.values())
        if total_wallets == 0:
            log_err(f"No wallets for {part_label}!")
            return

        txs_per_wallet = min(MAX_NONCE_GAP, 80)
        wave = 0
        total_accepted = 0
        start_time = time.time()

        while _now_utc() < end_dt and not SHUTDOWN.is_set():
            wave += 1
            wave_start = time.time()

            # Check remaining time — don't start a wave if < 10s left
            remaining_secs = (end_dt - _now_utc()).total_seconds()
            if remaining_secs < 10:
                log(f"{part_label}: <10s remaining, stopping")
                break

            # Balance check periodically
            if wave % BALANCE_CHECK_INTERVAL == 1:
                total_balance = 0
                funded = 0
                for s in [0, 1, 2]:
                    for w in shard_wallets[s]:
                        b = self._get_balance(w.address.to_bech32())
                        total_balance += b
                        if b >= GAS_COST_PER_TX * 2:
                            funded += 1
                remaining_cap = total_balance // GAS_COST_PER_TX if GAS_COST_PER_TX > 0 else 0
                log(f"Balance check: {total_balance / 10**18:.4f} EGLD | {funded}/{total_wallets} funded | ~{remaining_cap:,} txs left")
                if funded == 0:
                    log_warn(f"{part_label}: All wallets depleted — stopping")
                    break

            # Generate cross-shard txs
            txs = self.generate_cross_shard_txs(shard_wallets, receiver_map, txs_per_wallet, tx_value)
            if not txs:
                log_warn(f"Wave {wave}: 0 txs generated — stopping")
                break

            # Broadcast
            sent = self._broadcast_chunks(f"P{part}-W{wave}", txs)
            total_accepted += sent
            wave_elapsed = time.time() - wave_start

            stat_key = f"p{part}_accepted"
            self.stats[stat_key] = total_accepted
            self.stats[f"p{part}_sent"] += len(txs)

            elapsed_total = time.time() - start_time
            tps = total_accepted / elapsed_total if elapsed_total > 0 else 0
            remaining_secs = (end_dt - _now_utc()).total_seconds()

            log(f"Wave {wave}: {sent}/{len(txs)} accepted ({wave_elapsed:.1f}s) | "
                f"Total: {total_accepted:,} | {tps:.0f} TPS | {remaining_secs:.0f}s left")

            # Brief pause between waves for nonce processing
            if remaining_secs > SLEEP_BETWEEN_WAVES:
                time.sleep(SLEEP_BETWEEN_WAVES)

        elapsed = time.time() - start_time
        avg_tps = total_accepted / elapsed if elapsed > 0 else 0
        log_ok(f"{part_label} COMPLETE: {total_accepted:,} cross-shard txs in {elapsed:.1f}s ({avg_tps:.0f} avg TPS)")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN ORCHESTRATION
# ═══════════════════════════════════════════════════════════════════════════════
def graceful_shutdown(_signum, _frame):
    log_warn("Shutdown signal received — exiting.")
    SHUTDOWN.set()
    os._exit(0)


def main():
    global ENDPOINT_ORDER

    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    header("CHALLENGE #3: CROSSOVER — AUTOMATED ORCHESTRATOR")
    log(f"Server time (UTC): {_now_utc().strftime('%Y-%m-%d %H:%M:%S')}")

    # Build endpoint order
    ENDPOINT_ORDER = build_endpoint_order()
    log(f"Endpoints: {len(ENDPOINT_ORDER)} configured")

    engine = CrossoverEngine()

    # ── Step 1: Load & validate ─────────────────────────────────────────
    header("LOADING WALLETS")
    engine.load_leader()
    engine.load_part1_wallets()
    engine.load_part2_wallets()
    engine.validate_no_overlap()
    engine.validate_cross_shard_possible(engine.shard_wallets_p1, "Part 1")
    engine.validate_cross_shard_possible(engine.shard_wallets_p2, "Part 2")

    p1_count = len(engine.all_wallets_p1)
    p2_count = len(engine.all_wallets_p2)
    log(f"Total: {p1_count} P1 wallets + {p2_count} P2 wallets = {p1_count + p2_count}")

    # Parse time windows
    part1_start = _parse_utc(PART1_START)
    part1_end = _parse_utc(PART1_END)
    part2_start = _parse_utc(PART2_START)
    part2_end = _parse_utc(PART2_END)

    log(f"Schedule:")
    log(f"  Funding check: {FUNDING_CHECK_START} UTC")
    log(f"  Part 1: {PART1_START} — {PART1_END} UTC")
    log(f"  Part 2: {PART2_START} — {PART2_END} UTC")

    # ── Step 2: Wait for challenge funding ──────────────────────────────
    if not engine.wait_for_funding():
        log_err("Shutdown before funding detected")
        return

    if SHUTDOWN.is_set():
        return

    # ── Step 3: Fund Part 1 wallets ─────────────────────────────────────
    engine.fund_wallets(engine.all_wallets_p1, LOCAL_BUDGET_PART1_EGLD, "P1")

    if SHUTDOWN.is_set():
        return

    # ── Step 4: Part 1 blast (16:00–16:30 UTC) ──────────────────────────
    engine.run_blast_phase(
        part=1,
        shard_wallets=engine.shard_wallets_p1,
        receiver_map=engine.cross_shard_receivers_p1,
        tx_value=TX_VALUE_PART1,
        start_dt=part1_start,
        end_dt=part1_end,
    )

    if SHUTDOWN.is_set():
        return

    # ── Step 5: Fund Part 2 wallets (during 16:30–17:00 break) ──────────
    log("Part 1 ended. Funding Part 2 wallets during break...")
    engine.fund_wallets(engine.all_wallets_p2, LOCAL_BUDGET_PART2_EGLD, "P2")

    if SHUTDOWN.is_set():
        return

    # ── Step 6: Part 2 blast (17:00–17:30 UTC) ──────────────────────────
    engine.run_blast_phase(
        part=2,
        shard_wallets=engine.shard_wallets_p2,
        receiver_map=engine.cross_shard_receivers_p2,
        tx_value=TX_VALUE_PART2,
        start_dt=part2_start,
        end_dt=part2_end,
    )

    # ── Final summary ───────────────────────────────────────────────────
    header("CHALLENGE #3 COMPLETE — FINAL SUMMARY")
    log(f"Part 1: {engine.stats['p1_accepted']:,} accepted / {engine.stats['p1_sent']:,} sent")
    log(f"Part 2: {engine.stats['p2_accepted']:,} accepted / {engine.stats['p2_sent']:,} sent")
    log(f"Funding: P1={engine.stats['funding_p1_sent']} txs, P2={engine.stats['funding_p2_sent']} txs")
    log(f"Total cross-shard txs: {engine.stats['p1_accepted'] + engine.stats['p2_accepted']:,}")
    log_ok("All done. Good luck!")


if __name__ == "__main__":
    main()
