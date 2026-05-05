"""
Agente MCP — Pilar 4: Célula Inteligente
Stack: Python + Anthropic SDK (SDK nativo para agentes de IA)
Células Go são acessadas via HTTP — agnóstico à linguagem.
"""
import asyncio, os, json, logging, httpx
from collections import deque
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
import anthropic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("agent-mcp")

CELL_PEDIDOS_URL      = os.getenv("CELL_PEDIDOS_URL",      "http://cell-pedidos:8000")
CELL_ESTOQUE_URL      = os.getenv("CELL_ESTOQUE_URL",      "http://cell-estoque:8000")
CELL_NOTIFICACOES_URL = os.getenv("CELL_NOTIFICACOES_URL", "http://cell-notificacoes:8000")
PROMETHEUS_URL        = os.getenv("PROMETHEUS_URL",        "http://prometheus:9090")
ANTHROPIC_API_KEY     = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL       = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
SHARD_ROUTER_URL      = os.getenv("SHARD_ROUTER_URL",     "http://shard-router:8080")
SAGA_HUB_URL          = os.getenv("SAGA_HUB_URL",         "http://saga-hub:9090")

client_ai = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

ALLOWED_CONTAINERS = {
    "cell-pedidos-s1-active", "cell-pedidos-s2-active", "cell-pedidos-s3-active",
    "cell-estoque-s1-active", "cell-estoque-s2-active", "cell-estoque-s3-active",
    "cell-notif-s1-active", "cell-notif-s2-active", "cell-notif-s3-active",
    "shard-router", "saga-hub", "data-sync",
}

# F5.1 — Shard-aware tools
SHARD_TOOLS = [
    {
        "name": "listar_status_shards",
        "description": "Lista saúde de todas as células de todos os shards via /router/cells",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "verificar_saga",
        "description": "Consulta status de uma saga específica",
        "input_schema": {
            "type": "object",
            "properties": {"saga_id": {"type": "string"}},
            "required": ["saga_id"]
        }
    },
    {
        "name": "iniciar_saga_pedido",
        "description": "Inicia uma saga de pedido via saga-hub",
        "input_schema": {
            "type": "object",
            "properties": {
                "cliente_id": {"type": "string"},
                "shard_id":   {"type": "string"},
                "payload":    {"type": "object"}
            },
            "required": ["cliente_id", "shard_id", "payload"]
        }
    },
    {
        "name": "reiniciar_celula",
        "description": "Reinicia célula específica via docker restart (apenas local/dev)",
        "input_schema": {
            "type": "object",
            "properties": {
                "container_name": {"type": "string"},
                "motivo":         {"type": "string"}
            },
            "required": ["container_name", "motivo"]
        }
    },
    {
        "name": "consultar_prometheus",
        "description": "Executa query PromQL e retorna resultados",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"]
        }
    }
]


async def executar_shard_tool(name: str, inputs: dict) -> str:
    async with httpx.AsyncClient(timeout=10.0) as http:
        try:
            if name == "listar_status_shards":
                r = await http.get(f"{SHARD_ROUTER_URL}/router/cells")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "verificar_saga":
                r = await http.get(f"{SAGA_HUB_URL}/saga/{inputs['saga_id']}")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "iniciar_saga_pedido":
                r = await http.post(f"{SAGA_HUB_URL}/saga/pedido", json=inputs)
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "reiniciar_celula":
                container = inputs["container_name"]
                if container not in ALLOWED_CONTAINERS:
                    return json.dumps({"error": f"container '{container}' not in allowlist"})
                proc = await asyncio.create_subprocess_exec(
                    "docker", "restart", container,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
                return json.dumps({
                    "stdout": stdout.decode(),
                    "stderr": stderr.decode(),
                    "returncode": proc.returncode,
                })
            elif name == "consultar_prometheus":
                r = await http.get(f"{PROMETHEUS_URL}/api/v1/query",
                    params={"query": inputs["query"]})
                data = r.json().get("data", {}).get("result", [])
                return json.dumps(data, ensure_ascii=False)
            return f"unknown tool: {name}"
        except Exception as e:
            return f"Erro ao executar {name}: {e}"


MCP_TOOLS = [
    {
        "name": "criar_pedido",
        "description": "Cria um novo pedido (célula Go). Inicia o fluxo SAGA via Kafka automaticamente.",
        "input_schema": {
            "type": "object",
            "properties": {
                "cliente_id": {"type": "string"},
                "itens": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "produto_id": {"type": "string"},
                            "quantidade": {"type": "integer"},
                            "preco_unitario": {"type": "number"},
                        },
                        "required": ["produto_id", "quantidade", "preco_unitario"],
                    },
                },
            },
            "required": ["cliente_id", "itens"],
        },
    },
    {
        "name": "consultar_pedido",
        "description": "Consulta o status de um pedido pelo ID.",
        "input_schema": {
            "type": "object",
            "properties": {"pedido_id": {"type": "string"}},
            "required": ["pedido_id"],
        },
    },
    {
        "name": "listar_pedidos",
        "description": "Lista os pedidos recentes.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "consultar_estoque",
        "description": "Consulta o estoque disponível de todos os produtos.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "repor_estoque",
        "description": "Repõe o estoque de um produto.",
        "input_schema": {
            "type": "object",
            "properties": {
                "produto_id": {"type": "string"},
                "quantidade": {"type": "integer"},
            },
            "required": ["produto_id", "quantidade"],
        },
    },
    {
        "name": "listar_notificacoes",
        "description": "Lista as notificações enviadas.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "verificar_saude_sistema",
        "description": "Health check de todas as células Go + métricas Prometheus.",
        "input_schema": {"type": "object", "properties": {}},
    },
]

# F5.1: todas as tools disponíveis ao Claude — MCP + shard-aware
ALL_TOOLS = MCP_TOOLS + SHARD_TOOLS


async def executar_tool(name: str, inputs: dict) -> str:
    shard_tool_names = {t["name"] for t in SHARD_TOOLS}
    if name in shard_tool_names:
        return await executar_shard_tool(name, inputs)
    async with httpx.AsyncClient(timeout=10.0) as http:
        try:
            if name == "criar_pedido":
                r = await http.post(f"{CELL_PEDIDOS_URL}/pedidos/", json=inputs)
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "consultar_pedido":
                r = await http.get(f"{CELL_PEDIDOS_URL}/pedidos/{inputs['pedido_id']}")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "listar_pedidos":
                r = await http.get(f"{CELL_PEDIDOS_URL}/pedidos/")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "consultar_estoque":
                r = await http.get(f"{CELL_ESTOQUE_URL}/estoque/")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "repor_estoque":
                r = await http.put(
                    f"{CELL_ESTOQUE_URL}/estoque/{inputs['produto_id']}/repor",
                    json={"quantidade": inputs["quantidade"]},
                )
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "listar_notificacoes":
                r = await http.get(f"{CELL_NOTIFICACOES_URL}/notificacoes/")
                return json.dumps(r.json(), ensure_ascii=False)
            elif name == "verificar_saude_sistema":
                return await _verificar_saude(http)
            else:
                return f"Tool desconhecida: {name}"
        except Exception as e:
            return f"Erro ao executar {name}: {e}"


async def _verificar_saude(http: httpx.AsyncClient) -> str:
    cells = {
        "pedidos":      CELL_PEDIDOS_URL,
        "estoque":      CELL_ESTOQUE_URL,
        "notificacoes": CELL_NOTIFICACOES_URL,
    }
    resultados = {}
    for nome, url in cells.items():
        try:
            r = await http.get(f"{url}/healthz/live", timeout=3.0)
            resultados[nome] = {
                "status": "healthy" if r.status_code == 200 else "degraded",
                "http_status": r.status_code,
                "response_ms": round(r.elapsed.total_seconds() * 1000, 1),
                "runtime": "Go 1.22",
            }
        except Exception as e:
            resultados[nome] = {"status": "unreachable", "error": str(e)}

    return json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cells": resultados,
        "event_bus": "Kafka (confluent-kafka-go)",
        "overall": "healthy" if all(v.get("status") == "healthy" for v in resultados.values()) else "degraded",
    }, ensure_ascii=False, indent=2)


async def executar_agente(prompt: str, historico: list = None) -> str:
    mensagens = (historico or []) + [{"role": "user", "content": prompt}]
    system = """Você é o Agente de Self-Healing da Empresa Componível Inteligente.
As células de negócio são escritas em Go e se comunicam via Kafka em 3 shards.
Você acessa o sistema via ferramentas MCP e tem capacidade de diagnosticar problemas,
consultar métricas Prometheus, verificar status de shards e reiniciar células com falha.
Responda em português brasileiro. Seja direto e objetivo."""

    for _ in range(10):
        response = await client_ai.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=2048,
            system=system,
            tools=ALL_TOOLS,
            messages=mensagens,
        )
        if response.stop_reason == "end_turn":
            return " ".join(b.text for b in response.content if hasattr(b, "text"))

        tool_results = []
        for bloco in response.content:
            if bloco.type == "tool_use":
                logger.info(f"tool: {bloco.name} inputs={bloco.input}")
                resultado = await executar_tool(bloco.name, bloco.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": bloco.id,
                    "content": resultado,
                })
        mensagens.append({"role": "assistant", "content": response.content})
        mensagens.append({"role": "user", "content": tool_results})

    return "Agente atingiu limite de iterações."


monitor_log: deque[dict] = deque(maxlen=100)


async def loop_monitoramento():
    """F5.1: self-healing autônomo com consciência de shards e anomaly detection."""
    from anomaly.detector import detector
    from scaling.predictor import predictors

    await asyncio.sleep(45)
    logger.info("Loop de monitoramento autônomo iniciado (Go cells + Kafka + self-healing)")

    while True:
        try:
            # F5.2: coleta anomalias com IsolationForest
            anomalia = await detector.run_once()

            # F5.3: previsão de scaling por célula
            previsoes = []
            for pred in predictors.values():
                previsoes.append(await pred.predict())

            entrada_log = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "anomalia": anomalia,
                "previsoes_scaling": previsoes,
            }

            if anomalia.get("anomaly"):
                # F5.1: self-healing — Claude diagnostica e age com shard awareness
                score = anomalia.get("score", 0)
                metricas = anomalia.get("metrics", {})
                logger.warning(f"Anomalia detectada (score={score:.3f}): {metricas}")

                resultado = await executar_agente(
                    f"ALERTA DE ANOMALIA DETECTADA pelo IsolationForest.\n"
                    f"Score de anomalia: {score:.4f} (mais negativo = mais anômalo).\n"
                    f"Métricas coletadas: {json.dumps(metricas, ensure_ascii=False)}.\n"
                    f"Use listar_status_shards para verificar saúde dos shards, "
                    f"consultar_prometheus para investigar as métricas suspeitas, "
                    f"e reiniciar_celula se identificar célula com falha. "
                    f"Reporte o diagnóstico e as ações tomadas."
                )
                entrada_log["tipo"] = "self-healing"
                entrada_log["resultado"] = resultado
                logger.info(f"Self-healing concluído: {resultado[:200]}")
            else:
                # Monitoramento normal — verifica shards periodicamente
                resultado = await executar_agente(
                    "Verifique o status de todos os shards com listar_status_shards. "
                    "Reporte brevemente a saúde do sistema."
                )
                entrada_log["tipo"] = "monitoramento"
                entrada_log["resultado"] = resultado

            monitor_log.append(entrada_log)

        except Exception as e:
            logger.error(f"Erro no monitoramento: {e}")

        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ANTHROPIC_API_KEY:
        asyncio.create_task(loop_monitoramento())
        logger.info("Agente MCP iniciado — self-healing autônomo ativo")
    else:
        logger.warning("ANTHROPIC_API_KEY não definida — modo passivo")
    yield


app = FastAPI(
    title="Agente MCP — PoC ECI (Go + Kafka)",
    version="2.1.0",
    lifespan=lifespan,
)


class PromptRequest(BaseModel):
    prompt: str
    historico: list = []


@app.post("/agente/executar")
async def executar(req: PromptRequest):
    resultado = await executar_agente(req.prompt, req.historico)
    return {"resultado": resultado, "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/agente/monitor-log")
async def ver_log():
    return {"entradas": monitor_log[-20:]}


@app.get("/agente/tools")
async def listar_tools():
    return {"tools": [{"name": t["name"], "description": t["description"]} for t in ALL_TOOLS]}


@app.get("/agente/anomalias")
async def anomalias():
    from anomaly.detector import detector
    result = await detector.run_once()
    return result


@app.get("/agente/scaling/previsao")
async def scaling_previsao():
    from scaling.predictor import predictors
    results = []
    for pred in predictors.values():
        results.append(await pred.predict())
    return {"previsoes": results}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "agent-mcp", "version": "2.1.0"}
