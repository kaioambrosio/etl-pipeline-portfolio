from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from config.settings import get_settings
from scripts.models import get_engine
from api.snapshot import generate_snapshot_safe

app = FastAPI(title="ETL Portfólio API", version="1.0.0")

settings = get_settings()
engine = get_engine()


def _parse_origins(origins_raw: str) -> List[str]:
    origins = [o.strip() for o in origins_raw.split(",") if o.strip()]
    return origins


origins = _parse_origins(getattr(settings, "api_cors_origins", "http://localhost:5173"))

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup_snapshot() -> None:
    limit = getattr(settings, "api_snapshot_limit", 5000)
    generate_snapshot_safe(limit=limit)


def _map_status_display(status_db: str) -> str:
    status_upper = (status_db or "").strip().upper()
    if status_upper == "PAGO":
        return "Pago"
    if status_upper == "PENDENTE":
        return "Pendente"
    if status_upper == "CANCELADO":
        return "Cancelado"
    if status_upper == "ATRASADO":
        return "Atrasado"
    if status_upper == "ERRO":
        return "Erro"
    return "Erro"


def _build_filters(
    ano: Optional[int],
    mes: Optional[int],
    categoria: Optional[str],
    status: Optional[str],
    produto: Optional[str],
    busca: Optional[str] = None,
    skip_fields: Optional[Set[str]] = None,
) -> Tuple[str, Dict[str, Any]]:
    clauses = []
    params: Dict[str, Any] = {}
    skip_fields = skip_fields or set()

    if ano is not None and "ano" not in skip_fields:
        clauses.append("ano_transacao = :ano")
        params["ano"] = ano
    if mes is not None and "mes" not in skip_fields:
        clauses.append("mes_transacao = :mes")
        params["mes"] = mes
    if categoria and "categoria" not in skip_fields:
        clauses.append("categoria = :categoria")
        params["categoria"] = categoria
    if status and "status" not in skip_fields:
        status_norm = status.strip().lower()
        if status_norm == "pago":
            clauses.append("status_pagamento = 'PAGO'")
        elif status_norm == "pendente":
            clauses.append("status_pagamento = 'PENDENTE'")
        elif status_norm == "erro":
            clauses.append("status_pagamento = 'ERRO'")
        elif status_norm == "cancelado":
            clauses.append("status_pagamento = 'CANCELADO'")
        elif status_norm == "atrasado":
            clauses.append("status_pagamento = 'ATRASADO'")
        else:
            clauses.append("status_pagamento = :status")
            params["status"] = status
    if produto and "produto" not in skip_fields:
        clauses.append("produto = :produto")
        params["produto"] = produto
    if busca and busca.strip() and "busca" not in skip_fields:
        clauses.append(
            "(cliente ILIKE :busca OR produto ILIKE :busca OR categoria ILIKE :busca)"
        )
        params["busca"] = f"%{busca.strip()}%"

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, params


def _combine_where(base_where: str, extra_clause: str) -> str:
    if not extra_clause:
        return base_where
    if base_where:
        return f"{base_where} AND {extra_clause}"
    return f"WHERE {extra_clause}"


def _fetch_metricas(where_sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    sql = f"""
        SELECT
            COUNT(*)::int AS quantidade_transacoes,
            COALESCE(SUM(valor), 0)::float AS valor_total,
            COALESCE(AVG(valor), 0)::float AS ticket_medio,
            COALESCE(SUM(CASE WHEN status_pagamento = 'PAGO' THEN 1 ELSE 0 END), 0)::int
                AS quantidade_pagas,
            COALESCE(
                ROUND(
                    100.0 * SUM(CASE WHEN status_pagamento = 'PAGO' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0)::numeric,
                    2
                ),
                0
            )::float AS percentual_pagas,
            COALESCE(
                AVG(EXTRACT(EPOCH FROM (data_processamento - data_transacao)) / 86400.0),
                0
            )::float AS tempo_medio_processamento,
            COALESCE(
                AVG(EXTRACT(EPOCH FROM (data_pagamento - data_transacao)) / 86400.0)
                    FILTER (WHERE data_pagamento IS NOT NULL),
                0
            )::float AS tempo_medio_pagamento
        FROM transacoes
        {where_sql}
    """
    return _fetch_one(sql, params)


def _resolve_periodo_atual(
    ano: Optional[int],
    mes: Optional[int],
    categoria: Optional[str],
    status: Optional[str],
    produto: Optional[str],
    busca: Optional[str],
) -> Optional[Dict[str, Any]]:
    base_where, base_params = _build_filters(
        None, None, categoria, status, produto, busca, skip_fields={"ano", "mes"}
    )

    if ano and mes:
        return {"tipo": "mes", "ano": ano, "mes": mes}
    if ano and not mes:
        return {"tipo": "ano", "ano": ano}

    if mes and not ano:
        where_mes = _combine_where(base_where, "mes_transacao = :mes")
        params = dict(base_params)
        params["mes"] = mes
        max_ano = _fetch_one(
            f"SELECT MAX(ano_transacao) AS ano FROM transacoes {where_mes}", params
        ).get("ano")
        if max_ano:
            return {"tipo": "mes", "ano": int(max_ano), "mes": mes}

    max_date = _fetch_one(
        f"SELECT MAX(data_transacao) AS max_data FROM transacoes {base_where}", base_params
    ).get("max_data")
    if not max_date:
        return None

    return {"tipo": "mes", "ano": max_date.year, "mes": max_date.month}


def _resolve_periodo_anterior(periodo: Dict[str, Any]) -> Dict[str, Any]:
    if periodo["tipo"] == "ano":
        return {"tipo": "ano", "ano": periodo["ano"] - 1}

    mes = int(periodo["mes"])
    ano = int(periodo["ano"])
    mes_anterior = mes - 1
    ano_anterior = ano
    if mes_anterior == 0:
        mes_anterior = 12
        ano_anterior -= 1
    return {"tipo": "mes", "ano": ano_anterior, "mes": mes_anterior}


def _periodo_label(periodo: Dict[str, Any]) -> str:
    return "vs mês anterior" if periodo.get("tipo") == "mes" else "vs ano anterior"


def _fetch_all(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params).mappings().all()
    return [dict(row) for row in result]


def _fetch_one(sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params).mappings().first()
    return dict(result) if result else {}


@app.get("/health")
def health() -> Dict[str, Any]:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "db": "ok", "timestamp": datetime.utcnow().isoformat()}
    except Exception as exc:
        return {"status": "degraded", "db": "error", "error": str(exc)}


@app.get("/filtros")
def filtros(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
) -> Dict[str, List[Any]]:
    anos_where, anos_params = _build_filters(
        ano, mes, categoria, status, produto, skip_fields={"ano"}
    )
    meses_where, meses_params = _build_filters(
        ano, mes, categoria, status, produto, skip_fields={"mes"}
    )
    categorias_where, categorias_params = _build_filters(
        ano, mes, categoria, status, produto, skip_fields={"categoria"}
    )
    status_where, status_params = _build_filters(
        ano, mes, categoria, status, produto, skip_fields={"status"}
    )
    produtos_where, produtos_params = _build_filters(
        ano, mes, categoria, status, produto, skip_fields={"produto"}
    )

    anos = _fetch_all(
        f"SELECT DISTINCT ano_transacao AS ano FROM transacoes {anos_where} ORDER BY ano DESC",
        anos_params,
    )
    meses = _fetch_all(
        f"SELECT DISTINCT mes_transacao AS mes FROM transacoes {meses_where} ORDER BY mes ASC",
        meses_params,
    )
    categorias = _fetch_all(
        f"SELECT DISTINCT categoria FROM transacoes {categorias_where} ORDER BY categoria ASC",
        categorias_params,
    )
    status = _fetch_all(
        f"SELECT DISTINCT status_pagamento FROM transacoes {status_where}",
        status_params,
    )
    produtos = _fetch_all(
        f"SELECT DISTINCT produto FROM transacoes {produtos_where} ORDER BY produto ASC",
        produtos_params,
    )

    status_display = {_map_status_display(r["status_pagamento"]) for r in status}
    status_order = {
        "Pago": 0,
        "Pendente": 1,
        "Atrasado": 2,
        "Cancelado": 3,
        "Erro": 4,
    }

    return {
        "anos": [r["ano"] for r in anos],
        "meses": [r["mes"] for r in meses],
        "categorias": [r["categoria"] for r in categorias],
        "statusPagamento": sorted(status_display, key=lambda s: status_order.get(s, 99)),
        "produtos": [r["produto"] for r in produtos],
    }


@app.get("/transacoes")
def transacoes(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
    busca: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(default=True),
) -> Dict[str, Any]:
    where_sql, params = _build_filters(ano, mes, categoria, status, produto, busca)
    count_params = dict(params)
    params.update({"limit": limit, "offset": offset})

    sql = f"""
        SELECT
            id,
            id_transacao,
            cliente,
            produto,
            categoria,
            valor::float AS valor,
            CASE
                WHEN status_pagamento = 'PAGO' THEN 'Pago'
                WHEN status_pagamento = 'PENDENTE' THEN 'Pendente'
                WHEN status_pagamento = 'CANCELADO' THEN 'Cancelado'
                WHEN status_pagamento = 'ATRASADO' THEN 'Atrasado'
                WHEN status_pagamento = 'ERRO' THEN 'Erro'
                ELSE 'Erro'
            END AS status_pagamento,
            arquivo_origem,
            data_transacao,
            data_processamento,
            data_pagamento,
            dia_semana,
            mes_transacao,
            trimestre,
            ano_transacao
        FROM transacoes
        {where_sql}
        ORDER BY data_transacao DESC
        LIMIT :limit OFFSET :offset
    """

    rows = _fetch_all(sql, params)
    total = None
    if include_total:
        total = _fetch_one(
            f"SELECT COUNT(*)::int AS total FROM transacoes {where_sql}",
            count_params,
        ).get("total", 0)
    return {"items": rows, "limit": limit, "offset": offset, "total": total}


@app.get("/transacoes/total")
def transacoes_total(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
    busca: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    where_sql, params = _build_filters(ano, mes, categoria, status, produto, busca)
    total = _fetch_one(
        f"SELECT COUNT(*)::int AS total FROM transacoes {where_sql}",
        params,
    ).get("total", 0)
    return {"total": total}


@app.get("/transacoes/{id_transacao}")
def transacao_detalhe(id_transacao: str) -> Dict[str, Any]:
    transacao = _fetch_one(
        """
        SELECT
            id,
            id_transacao,
            cliente,
            produto,
            categoria,
            valor::float AS valor,
            CASE
                WHEN status_pagamento = 'PAGO' THEN 'Pago'
                WHEN status_pagamento = 'PENDENTE' THEN 'Pendente'
                WHEN status_pagamento = 'CANCELADO' THEN 'Cancelado'
                WHEN status_pagamento = 'ATRASADO' THEN 'Atrasado'
                WHEN status_pagamento = 'ERRO' THEN 'Erro'
                ELSE 'Erro'
            END AS status_pagamento,
            arquivo_origem,
            data_transacao,
            data_processamento,
            data_pagamento,
            dia_semana,
            mes_transacao,
            trimestre,
            ano_transacao
        FROM transacoes
        WHERE id_transacao = :id_transacao
        """,
        {"id_transacao": id_transacao},
    )

    if not transacao:
        return {"transacao": None, "itens": []}

    itens = _fetch_all(
        """
        SELECT
            i.id_transacao,
            p.nome AS produto,
            c.nome AS categoria,
            p.descricao AS produto_descricao,
            i.quantidade,
            i.valor_unitario::float AS valor_unitario,
            i.valor_total::float AS valor_total
        FROM transacao_itens i
        JOIN produtos p ON p.id = i.produto_id
        JOIN categorias c ON c.id = p.categoria_id
        WHERE i.id_transacao = :id_transacao
        ORDER BY i.valor_total DESC
        """,
        {"id_transacao": id_transacao},
    )

    return {"transacao": transacao, "itens": itens}


@app.get("/metricas")
def metricas(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    where_sql, params = _build_filters(ano, mes, categoria, status, produto)
    return _fetch_metricas(where_sql, params)


@app.get("/metricas/comparativo")
def metricas_comparativo(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
    busca: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    periodo_atual = _resolve_periodo_atual(ano, mes, categoria, status, produto, busca)
    if not periodo_atual:
        return {
            "valorTotalAnterior": 0,
            "quantidadeAnterior": 0,
            "variacaoValor": 0,
            "variacaoQuantidade": 0,
            "label": "vs período anterior",
            "periodoAtual": None,
            "periodoAnterior": None,
        }

    periodo_anterior = _resolve_periodo_anterior(periodo_atual)
    label = _periodo_label(periodo_atual)

    base_where, base_params = _build_filters(
        None, None, categoria, status, produto, busca, skip_fields={"ano", "mes"}
    )

    if periodo_atual["tipo"] == "ano":
        where_atual = _combine_where(base_where, "ano_transacao = :ano_atual")
        params_atual = dict(base_params)
        params_atual["ano_atual"] = periodo_atual["ano"]

        where_anterior = _combine_where(base_where, "ano_transacao = :ano_anterior")
        params_anterior = dict(base_params)
        params_anterior["ano_anterior"] = periodo_anterior["ano"]
    else:
        where_atual = _combine_where(
            base_where, "ano_transacao = :ano_atual AND mes_transacao = :mes_atual"
        )
        params_atual = dict(base_params)
        params_atual["ano_atual"] = periodo_atual["ano"]
        params_atual["mes_atual"] = periodo_atual["mes"]

        where_anterior = _combine_where(
            base_where,
            "ano_transacao = :ano_anterior AND mes_transacao = :mes_anterior",
        )
        params_anterior = dict(base_params)
        params_anterior["ano_anterior"] = periodo_anterior["ano"]
        params_anterior["mes_anterior"] = periodo_anterior["mes"]

    metricas_atual = _fetch_metricas(where_atual, params_atual)
    metricas_anterior = _fetch_metricas(where_anterior, params_anterior)

    valor_atual = metricas_atual.get("valor_total", 0) or 0
    valor_anterior = metricas_anterior.get("valor_total", 0) or 0
    qtd_atual = metricas_atual.get("quantidade_transacoes", 0) or 0
    qtd_anterior = metricas_anterior.get("quantidade_transacoes", 0) or 0

    variacao_valor = (
        ((valor_atual - valor_anterior) / valor_anterior) * 100
        if valor_anterior > 0
        else 0
    )
    variacao_quantidade = (
        ((qtd_atual - qtd_anterior) / qtd_anterior) * 100 if qtd_anterior > 0 else 0
    )

    return {
        "valorTotalAnterior": valor_anterior,
        "quantidadeAnterior": qtd_anterior,
        "variacaoValor": variacao_valor,
        "variacaoQuantidade": variacao_quantidade,
        "label": label,
        "periodoAtual": periodo_atual,
        "periodoAnterior": periodo_anterior,
    }


@app.get("/agregados/categorias")
def agregados_categorias(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
) -> List[Dict[str, Any]]:
    where_sql, params = _build_filters(ano, mes, None, status, produto)

    sql = f"""
        SELECT
            categoria,
            COUNT(*)::int AS quantidade,
            COALESCE(SUM(valor), 0)::float AS valor
        FROM transacoes
        {where_sql}
        GROUP BY categoria
        ORDER BY valor DESC
    """

    return _fetch_all(sql, params)


@app.get("/agregados/volume-mensal")
def agregados_volume_mensal(
    ano: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
) -> List[Dict[str, Any]]:
    where_sql, params = _build_filters(ano, None, categoria, status, produto)

    sql = f"""
        SELECT
            ano_transacao AS ano,
            mes_transacao AS mes,
            COUNT(*)::int AS quantidade,
            COALESCE(SUM(valor), 0)::float AS valor
        FROM transacoes
        {where_sql}
        GROUP BY ano_transacao, mes_transacao
        ORDER BY ano_transacao, mes_transacao
    """

    return _fetch_all(sql, params)


@app.get("/agregados/dia-semana")
def agregados_dia_semana(
    ano: Optional[int] = Query(default=None),
    mes: Optional[int] = Query(default=None),
    categoria: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    produto: Optional[str] = Query(default=None),
) -> List[Dict[str, Any]]:
    where_sql, params = _build_filters(ano, mes, categoria, status, produto)

    sql = f"""
        SELECT
            dia_semana,
            CASE dia_semana
                WHEN 0 THEN 'Segunda'
                WHEN 1 THEN 'Terça'
                WHEN 2 THEN 'Quarta'
                WHEN 3 THEN 'Quinta'
                WHEN 4 THEN 'Sexta'
                WHEN 5 THEN 'Sábado'
                WHEN 6 THEN 'Domingo'
            END AS dia,
            COUNT(*)::int AS quantidade,
            COALESCE(SUM(valor), 0)::float AS valor
        FROM transacoes
        {where_sql}
        GROUP BY dia_semana
        ORDER BY dia_semana
    """

    return _fetch_all(sql, params)
