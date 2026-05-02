"""
Agente MCP — Pilar 4: Célula Inteligente
Stack: Python + Anthropic SDK (SDK nativo para agentes de IA)
Células Go são acessadas via HTTP — agnóstico à linguagem.
"""
import asyncio, os, json, logging, httpx
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

client_ai = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

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


async def executar_tool(name: str, inputs: dict) -> str:
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
            r = await http.get(f"{url}/health", timeout=3.0)
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
    system = """Você é o Agente de Monitoramento da Empresa Componível Inteligente.
As células de negócio são escritas em Go e se comunicam via Kafka.
Você as acessa via HTTP através de ferramentas MCP.
Responda em português brasileiro. Seja direto e objetivo."""

    for _ in range(10):
        response = await client_ai.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=MCP_TOOLS,
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


monitor_log: list[dict] = []

async def loop_monitoramento():
    await asyncio.sleep(45)
    logger.info("Loop de monitoramento autônomo iniciado (Go cells + Kafka)")
    while True:
        try:
            resultado = await executar_agente(
                "Verifique a saúde de todas as células Go. "
                "Se alguma estiver degradada, diagnostique e recomende ação corretiva."
            )
            monitor_log.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "resultado": resultado,
            })
            if len(monitor_log) > 100:
                monitor_log.pop(0)
        except Exception as e:
            logger.error(f"Erro no monitoramento: {e}")
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ANTHROPIC_API_KEY:
        asyncio.create_task(loop_monitoramento())
        logger.info("Agente MCP iniciado — monitorando células Go via HTTP")
    else:
        logger.warning("ANTHROPIC_API_KEY não definida — modo passivo")
    yield


app = FastAPI(
    title="Agente MCP — PoC ECI (Go + Kafka)",
    version="2.0.0",
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
    return {"tools": [{"name": t["name"], "description": t["description"]} for t in MCP_TOOLS]}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "agent-mcp", "version": "2.0.0"}
