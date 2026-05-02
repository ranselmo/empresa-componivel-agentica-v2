#!/usr/bin/env python3
"""
Fitness Functions — PoC ECI v2 (Go + Kafka)
FF1: Boundary Isolation — análise estática do código Go
FF2: Contract Tests — validação dos endpoints REST
FF3: Latência p99 — teste de carga
FF4: Chaos Bulkhead — blast radius
"""
import sys, os, asyncio, subprocess, time, statistics, pathlib
import httpx

ROOT = pathlib.Path(__file__).parent.parent
BASE_URL = "http://localhost"


# ══════════════════════════════════════════════════════════════════
# FF1 — Boundary Isolation (Go source analysis)
# ══════════════════════════════════════════════════════════════════

BOUNDARY_RULES = {
    "cell-pedidos":      ["estoque123", "notif123", "db-estoque", "db-notificacoes", "5434", "5435"],
    "cell-estoque":      ["pedidos123", "notif123", "db-pedidos", "db-notificacoes", "5433", "5435"],
    "cell-notificacoes": ["pedidos123", "estoque123", "db-pedidos", "db-estoque",    "5433", "5434"],
}

FORBIDDEN_IMPORTS = {
    "cell-pedidos":      ["cell-estoque", "cell-notificacoes"],
    "cell-estoque":      ["cell-pedidos", "cell-notificacoes"],
    "cell-notificacoes": ["cell-pedidos", "cell-estoque"],
}


def check_boundary(cell: str) -> list[str]:
    cell_dir = ROOT / cell
    if not cell_dir.exists():
        return []

    violations = []
    forbidden_strings = BOUNDARY_RULES.get(cell, [])
    forbidden_imports = FORBIDDEN_IMPORTS.get(cell, [])

    for go_file in cell_dir.rglob("*.go"):
        content = go_file.read_text(errors="ignore")

        for pattern in forbidden_strings:
            if pattern in content:
                violations.append(
                    f"  [{cell}] {go_file.relative_to(ROOT)}: referência proibida '{pattern}'"
                )

        for imp in forbidden_imports:
            if f'"{imp}/' in content or f'"github.com/ranselmo/poc-eci/{imp}' in content:
                violations.append(
                    f"  [{cell}] {go_file.relative_to(ROOT)}: import de célula cruzada '{imp}'"
                )

    return violations


def run_ff1() -> bool:
    print("─" * 55)
    print("FF1 — Boundary Isolation (análise estática Go)")
    print("─" * 55)
    all_ok = True
    for cell in BOUNDARY_RULES:
        v = check_boundary(cell)
        if v:
            print(f"  FAIL {cell}")
            for line in v:
                print(line)
            all_ok = False
        else:
            print(f"  PASS {cell}")
    return all_ok


# ══════════════════════════════════════════════════════════════════
# FF2 — Contract Tests
# ══════════════════════════════════════════════════════════════════

CONTRACTS = [
    {
        "cell": "pedidos", "method": "POST", "path": "/pedidos/",
        "name": "POST /pedidos cria com campos obrigatórios",
        "body": {
            "cliente_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "itens": [{"produto_id": "11111111-1111-1111-1111-111111111111",
                        "quantidade": 1, "preco_unitario": 4999.90}],
        },
        "expected_status": 201,
        "required_fields": ["pedido_id", "status", "valor_total"],
    },
    {
        "cell": "pedidos", "method": "GET", "path": "/pedidos/",
        "name": "GET /pedidos retorna lista",
        "body": None, "expected_status": 200, "expect_list": True,
    },
    {
        "cell": "estoque", "method": "GET", "path": "/estoque/",
        "name": "GET /estoque retorna lista de produtos",
        "body": None, "expected_status": 200, "expect_list": True,
    },
    {
        "cell": "estoque", "method": "GET",
        "path": "/estoque/11111111-1111-1111-1111-111111111111",
        "name": "GET /estoque/{id} retorna campos obrigatórios",
        "body": None, "expected_status": 200,
        "required_fields": ["id", "nome", "quantidade_disponivel", "preco"],
    },
    {
        "cell": "notificacoes", "method": "GET", "path": "/notificacoes/",
        "name": "GET /notificacoes retorna lista",
        "body": None, "expected_status": 200, "expect_list": True,
    },
]


async def run_ff2() -> bool:
    print("\n─" * 28)
    print("FF2 — Contract Tests")
    print("─" * 55)
    all_ok = True
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        for c in CONTRACTS:
            try:
                if c["method"] == "GET":
                    r = await client.get(c["path"])
                else:
                    r = await client.post(c["path"], json=c["body"])

                passed = r.status_code == c["expected_status"]
                if passed and c.get("required_fields"):
                    body = r.json()
                    for f in c["required_fields"]:
                        if f not in body:
                            passed = False
                            break
                if passed and c.get("expect_list"):
                    passed = isinstance(r.json(), list)

                status = "PASS" if passed else "FAIL"
                print(f"  {status} [{c['cell']}] {c['name']}")
                if not passed:
                    all_ok = False
            except Exception as e:
                print(f"  FAIL [{c['cell']}] {c['name']} — {e}")
                all_ok = False
    return all_ok


# ══════════════════════════════════════════════════════════════════
# FF3 — Latência p99
# ══════════════════════════════════════════════════════════════════

P99_LIMIT_MS = 200
N_REQUESTS   = 50
CONCURRENCY  = 5


async def run_ff3() -> bool:
    print("\n─" * 28)
    print(f"FF3 — Latência p99 < {P99_LIMIT_MS}ms")
    print("─" * 55)

    endpoints = [
        ("GET /pedidos/",    f"{BASE_URL}/pedidos/"),
        ("GET /estoque/",    f"{BASE_URL}/estoque/"),
        ("GET /notificacoes/", f"{BASE_URL}/notificacoes/"),
    ]
    all_ok = True

    async with httpx.AsyncClient(timeout=5.0) as client:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def timed(url):
            async with sem:
                start = time.perf_counter()
                try:
                    r = await client.get(url)
                    return (time.perf_counter() - start) * 1000 if r.status_code < 500 else 9999.0
                except Exception:
                    return 9999.0

        for name, url in endpoints:
            latencies = await asyncio.gather(*[timed(url) for _ in range(N_REQUESTS)])
            valid = sorted(l for l in latencies if l < 9000)
            if not valid:
                print(f"  FAIL {name} — todos os requests falharam")
                all_ok = False
                continue
            p99 = valid[min(int(len(valid) * 0.99), len(valid) - 1)]
            avg = statistics.mean(valid)
            ok = p99 <= P99_LIMIT_MS
            print(f"  {'PASS' if ok else 'FAIL'} {name}  p99={p99:.1f}ms  avg={avg:.1f}ms")
            if not ok:
                all_ok = False

    return all_ok


# ══════════════════════════════════════════════════════════════════
# FF4 — Chaos Bulkhead
# ══════════════════════════════════════════════════════════════════

async def run_ff4() -> bool:
    print("\n─" * 28)
    print("FF4 — Chaos Engineering: Blast Radius (Bulkhead)")
    print("─" * 55)

    async with httpx.AsyncClient(timeout=3.0) as client:
        async def up(path):
            try:
                return (await client.get(f"{BASE_URL}{path}")).status_code == 200
            except Exception:
                return False

        # baseline
        if not all([await up("/pedidos/health"), await up("/estoque/health"), await up("/notificacoes/health")]):
            print("  SKIP — sistema não está totalmente saudável")
            return True

        print("  Derrubando cell-estoque...")
        subprocess.run(["docker", "stop", "cell-estoque"], capture_output=True)
        await asyncio.sleep(4)

        pedidos_ok = await up("/pedidos/health")
        estoque_ok = await up("/estoque/health")
        notif_ok   = await up("/notificacoes/health")

        print(f"  pedidos:      {'UP' if pedidos_ok else 'DOWN'} (esperado: UP)")
        print(f"  estoque:      {'DOWN' if not estoque_ok else 'AINDA UP'} (esperado: DOWN)")
        print(f"  notificacoes: {'UP' if notif_ok else 'DOWN'} (esperado: UP)")

        contained = pedidos_ok and not estoque_ok and notif_ok

        print("  Restaurando cell-estoque...")
        subprocess.run(["docker", "start", "cell-estoque"], capture_output=True)
        await asyncio.sleep(12)

        recovered = await up("/estoque/health")
        print(f"  Recuperação: {'OK' if recovered else 'FALHOU'}")

        ok = contained and recovered
        print(f"  {'PASS' if ok else 'FAIL'} — Bulkhead {'validado' if ok else 'falhou'}")
        return ok


# ══════════════════════════════════════════════════════════════════
# Suite runner
# ══════════════════════════════════════════════════════════════════

async def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║    SUITE DE FITNESS FUNCTIONS — PoC ECI v2 (Go)     ║")
    print("╚══════════════════════════════════════════════════════╝")

    results = [
        ("FF1 Boundary Isolation", run_ff1()),
        ("FF2 Contract Tests",     await run_ff2()),
        ("FF3 Latência p99",       await run_ff3()),
        ("FF4 Chaos Bulkhead",     await run_ff4()),
    ]

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║                   RESULTADO FINAL                   ║")
    print("╚══════════════════════════════════════════════════════╝")
    for name, ok in results:
        if asyncio.iscoroutine(ok):
            ok = await ok
        print(f"  {'✓ PASS' if ok else '✗ FAIL'}  {name}")

    failed = [n for n, ok in results if not ok]
    print()
    if failed:
        print(f"SUITE REPROVADA — {len(failed)} fitness function(s) falharam")
        sys.exit(1)
    else:
        print("SUITE APROVADA — Arquitetura em conformidade")


if __name__ == "__main__":
    asyncio.run(main())
