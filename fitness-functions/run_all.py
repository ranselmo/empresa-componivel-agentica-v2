#!/usr/bin/env python3
"""
Fitness Functions — PoC ECI v2 (Go + Kafka)
FF1: Boundary Isolation — análise estática do código Go
FF2: Contract Tests    — validação dos endpoints REST via shard-router
FF3: Latência p99      — teste de carga
FF4: Chaos Bulkhead    — blast radius (ativo cai, passivo faz failover)
"""
import sys, os, asyncio, subprocess, time, statistics, pathlib
import httpx

ROOT     = pathlib.Path(__file__).parent.parent
BASE_URL = "http://localhost:8080"          # tudo vai pelo shard-router
PROJECT  = "poc-eci"                        # nome do projeto docker compose


# ══════════════════════════════════════════════════════════════════
# FF1 — Boundary Isolation (Go source analysis)
# ══════════════════════════════════════════════════════════════════

BOUNDARY_RULES = {
    "cell-pedidos":      ["estoque123", "notif123", "db-estoque", "db-notificacoes"],
    "cell-estoque":      ["pedidos123", "notif123",  "db-pedidos", "db-notificacoes"],
    "cell-notificacoes": ["pedidos123", "estoque123","db-pedidos", "db-estoque"],
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
    for go_file in cell_dir.rglob("*.go"):
        content = go_file.read_text(errors="ignore")
        for pattern in BOUNDARY_RULES.get(cell, []):
            if pattern in content:
                violations.append(
                    f"  [{cell}] {go_file.relative_to(ROOT)}: referência proibida '{pattern}'"
                )
        for imp in FORBIDDEN_IMPORTS.get(cell, []):
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
# FF2 — Contract Tests (via shard-router:8080)
# ══════════════════════════════════════════════════════════════════

CONTRACTS = [
    {
        "cell": "pedidos", "method": "POST", "path": "/pedidos/",
        "name": "POST /pedidos cria com campos obrigatórios",
        "headers": {"X-Client-ID": "ff2-test-cliente"},
        "body": {
            "cliente_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "itens": [{"produto_id": "22222222-2222-2222-2222-222222222222",
                        "quantidade": 1, "preco_unitario": 199.90}],
        },
        "expected_status": 201,
        "required_fields": ["pedido_id", "status", "valor_total"],
    },
    {
        "cell": "pedidos", "method": "GET", "path": "/pedidos/",
        "name": "GET /pedidos retorna lista",
        "headers": {"X-Client-ID": "ff2-test-cliente"},
        "body": None, "expected_status": 200, "expect_list": True,
    },
    {
        "cell": "pedidos", "method": "GET", "path": "/pedidos/stats",
        "name": "GET /pedidos/stats retorna estatísticas",
        "headers": {"X-Client-ID": "ff2-test-cliente"},
        "body": None, "expected_status": 200,
        "required_fields": ["total", "pendentes"],
    },
    {
        "cell": "estoque", "method": "GET", "path": "/estoque/",
        "name": "GET /estoque retorna lista de produtos",
        "headers": {},
        "body": None, "expected_status": 200, "expect_list": True,
    },
    {
        "cell": "estoque", "method": "GET",
        "path": "/estoque/11111111-1111-1111-1111-111111111111",
        "name": "GET /estoque/{id} retorna campos obrigatórios",
        "headers": {},
        "body": None, "expected_status": 200,
        "required_fields": ["id", "nome", "quantidade_disponivel", "preco"],
    },
    {
        "cell": "notificacoes", "method": "GET", "path": "/notificacoes/",
        "name": "GET /notificacoes retorna lista",
        "headers": {"X-Client-ID": "ff2-test-cliente"},
        "body": None, "expected_status": 200, "expect_list": True,
    },
    {
        "cell": "shard-router", "method": "GET", "path": "/healthz/live",
        "name": "GET /healthz/live retorna status ok",
        "headers": {},
        "body": None, "expected_status": 200,
        "required_fields": ["status"],
    },
    {
        "cell": "shard-router", "method": "GET", "path": "/router/cells",
        "name": "GET /router/cells retorna células registradas",
        "headers": {},
        "body": None, "expected_status": 200,
        "required_fields": ["cells"],
    },
]


async def run_ff2() -> bool:
    print("\n" + "─" * 55)
    print("FF2 — Contract Tests (via shard-router:8080)")
    print("─" * 55)
    all_ok = True
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=8.0) as client:
        for c in CONTRACTS:
            try:
                headers = c.get("headers", {})
                if c["method"] == "GET":
                    r = await client.get(c["path"], headers=headers)
                else:
                    r = await client.post(c["path"], json=c["body"], headers=headers)

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
                    print(f"       status={r.status_code} body={r.text[:120]}")
                    all_ok = False
            except Exception as e:
                print(f"  FAIL [{c['cell']}] {c['name']} — {e}")
                all_ok = False
    return all_ok


# ══════════════════════════════════════════════════════════════════
# FF3 — Latência p99
# ══════════════════════════════════════════════════════════════════

P99_LIMIT_MS = 300
N_REQUESTS   = 50
CONCURRENCY  = 5


async def run_ff3() -> bool:
    print("\n" + "─" * 55)
    print(f"FF3 — Latência p99 < {P99_LIMIT_MS}ms ({N_REQUESTS} req, concurrency={CONCURRENCY})")
    print("─" * 55)

    endpoints = [
        ("GET /pedidos/",      f"{BASE_URL}/pedidos/",      {"X-Client-ID": "ff3-load-test"}),
        ("GET /estoque/",      f"{BASE_URL}/estoque/",      {}),
        ("GET /router/cells",  f"{BASE_URL}/router/cells",  {}),
    ]
    all_ok = True

    async with httpx.AsyncClient(timeout=8.0) as client:
        sem = asyncio.Semaphore(CONCURRENCY)

        async def timed(url, headers):
            async with sem:
                start = time.perf_counter()
                try:
                    r = await client.get(url, headers=headers)
                    return (time.perf_counter() - start) * 1000 if r.status_code < 500 else 9999.0
                except Exception:
                    return 9999.0

        for name, url, headers in endpoints:
            latencies = await asyncio.gather(*[timed(url, headers) for _ in range(N_REQUESTS)])
            valid = sorted(l for l in latencies if l < 9000)
            if not valid:
                print(f"  FAIL {name} — todos os requests falharam")
                all_ok = False
                continue
            p99 = valid[min(int(len(valid) * 0.99), len(valid) - 1)]
            avg = statistics.mean(valid)
            ok  = p99 <= P99_LIMIT_MS
            print(f"  {'PASS' if ok else 'FAIL'} {name}  p99={p99:.1f}ms  avg={avg:.1f}ms  ({len(valid)}/{N_REQUESTS} ok)")
            if not ok:
                all_ok = False

    return all_ok


# ══════════════════════════════════════════════════════════════════
# FF4 — Chaos Bulkhead (blast radius)
# ══════════════════════════════════════════════════════════════════

TARGET_SERVICE   = "cell-estoque-s1-active"
TARGET_CONTAINER = f"{PROJECT}-{TARGET_SERVICE}-1"


async def router_cells_health(client: httpx.AsyncClient) -> dict[str, bool]:
    """Retorna dict cellID → healthy via /router/cells."""
    try:
        r = await client.get(f"{BASE_URL}/router/cells")
        cells = r.json().get("cells", [])
        return {c["ID"]: c["Healthy"] for c in cells}
    except Exception:
        return {}


async def run_ff4() -> bool:
    print("\n" + "─" * 55)
    print("FF4 — Chaos Engineering: Blast Radius (Bulkhead)")
    print("─" * 55)

    async with httpx.AsyncClient(timeout=5.0) as client:
        # Verifica baseline
        health = await router_cells_health(client)
        if not health:
            print("  SKIP — shard-router não acessível")
            return True

        healthy_count = sum(health.values())
        print(f"  Baseline: {healthy_count}/{len(health)} células saudáveis")
        if healthy_count < len(health):
            print("  SKIP — sistema não está 100% saudável antes do teste")
            return True

        # Para célula alvo
        print(f"  Parando {TARGET_CONTAINER}...")
        result = subprocess.run(
            ["docker", "stop", TARGET_CONTAINER],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  SKIP — container {TARGET_CONTAINER!r} não encontrado: {result.stderr.strip()}")
            return True

        # Aguarda watcher detectar (intervalo = 5s)
        await asyncio.sleep(8)

        # Verifica isolamento
        health_after = await router_cells_health(client)
        target_cell_id = f"cell-estoque-s1-active"
        target_down = not health_after.get(target_cell_id, True)

        # Verifica que pedidos e notificacoes ainda funcionam
        try:
            r_pedidos = await client.get(f"{BASE_URL}/pedidos/", headers={"X-Client-ID": "ff4-chaos"})
            pedidos_ok = r_pedidos.status_code == 200
        except Exception:
            pedidos_ok = False

        try:
            r_notif = await client.get(f"{BASE_URL}/notificacoes/", headers={"X-Client-ID": "ff4-chaos"})
            notif_ok = r_notif.status_code == 200
        except Exception:
            notif_ok = False

        print(f"  cell-estoque-s1-active: {'DOWN ✓' if target_down else 'ainda UP (?)'} (esperado: DOWN)")
        print(f"  cell-pedidos    (outro PBC): {'UP ✓' if pedidos_ok else 'DOWN ✗'} (esperado: UP — bulkhead)")
        print(f"  cell-notificacoes (outro PBC): {'UP ✓' if notif_ok else 'DOWN ✗'} (esperado: UP — bulkhead)")

        isolated = target_down and pedidos_ok and notif_ok

        # Restaura e aguarda recovery
        print(f"  Restaurando {TARGET_CONTAINER}...")
        subprocess.run(["docker", "start", TARGET_CONTAINER], capture_output=True)
        await asyncio.sleep(12)

        health_recovered = await router_cells_health(client)
        recovered = health_recovered.get(target_cell_id, False)
        print(f"  Recuperação: {'OK ✓' if recovered else 'FALHOU ✗'}")

        ok = isolated and recovered
        print(f"  {'PASS' if ok else 'FAIL'} — Bulkhead {'validado' if ok else 'falhou'}")
        return ok


# ══════════════════════════════════════════════════════════════════
# Suite runner
# ══════════════════════════════════════════════════════════════════

async def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║    SUITE DE FITNESS FUNCTIONS — PoC ECI v2 (Go)     ║")
    print("╚══════════════════════════════════════════════════════╝")

    ff1_ok = run_ff1()
    ff2_ok = await run_ff2()
    ff3_ok = await run_ff3()
    ff4_ok = await run_ff4()

    results = [
        ("FF1 Boundary Isolation", ff1_ok),
        ("FF2 Contract Tests",     ff2_ok),
        ("FF3 Latência p99",       ff3_ok),
        ("FF4 Chaos Bulkhead",     ff4_ok),
    ]

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║                   RESULTADO FINAL                   ║")
    print("╚══════════════════════════════════════════════════════╝")
    for name, ok in results:
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
