import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

from scripts.models import get_engine


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


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _snapshot_path() -> Path:
    etl_root = Path(__file__).resolve().parents[1]
    dashboard_public = etl_root / "dashboard" / "public"
    dashboard_public.mkdir(parents=True, exist_ok=True)
    return dashboard_public / "mock.json"


def generate_snapshot(limit: int = 2000) -> Dict[str, Any]:
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
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
                ORDER BY data_transacao DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).mappings()
        items = [dict(r) for r in rows]

        anos = conn.execute(
            text("SELECT DISTINCT ano_transacao AS ano FROM transacoes ORDER BY ano DESC")
        ).mappings()
        meses = conn.execute(
            text("SELECT DISTINCT mes_transacao AS mes FROM transacoes ORDER BY mes ASC")
        ).mappings()
        categorias = conn.execute(
            text("SELECT DISTINCT categoria FROM transacoes ORDER BY categoria ASC")
        ).mappings()
        status_raw = conn.execute(
            text("SELECT DISTINCT status_pagamento FROM transacoes")
        ).mappings()
        produtos = conn.execute(
            text("SELECT DISTINCT produto FROM transacoes ORDER BY produto ASC")
        ).mappings()

        status_display = {_map_status_display(r["status_pagamento"]) for r in status_raw}
        status_order = {
            "Pago": 0,
            "Pendente": 1,
            "Atrasado": 2,
            "Cancelado": 3,
            "Erro": 4,
        }

        metricas = conn.execute(
            text(
                """
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
                """
            )
        ).mappings().first()

        categorias_ag = conn.execute(
            text(
                """
                SELECT
                    categoria,
                    COUNT(*)::int AS quantidade,
                    COALESCE(SUM(valor), 0)::float AS valor
                FROM transacoes
                GROUP BY categoria
                ORDER BY valor DESC
                """
            )
        ).mappings().all()

        volume_mensal = conn.execute(
            text(
                """
                SELECT
                    ano_transacao AS ano,
                    mes_transacao AS mes,
                    COUNT(*)::int AS quantidade,
                    COALESCE(SUM(valor), 0)::float AS valor
                FROM transacoes
                GROUP BY ano_transacao, mes_transacao
                ORDER BY ano_transacao, mes_transacao
                """
            )
        ).mappings().all()

        dias_semana = conn.execute(
            text(
                """
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
                GROUP BY dia_semana
                ORDER BY dia_semana
                """
            )
        ).mappings().all()

    payload = {
        "generated_at": datetime.utcnow().isoformat(),
        "limit": limit,
        "transacoes": items,
        "filtros": {
            "anos": [r["ano"] for r in anos],
            "meses": [r["mes"] for r in meses],
            "categorias": [r["categoria"] for r in categorias],
            "statusPagamento": sorted(status_display, key=lambda s: status_order.get(s, 99)),
            "produtos": [r["produto"] for r in produtos],
        },
        "metricas": dict(metricas) if metricas else {},
        "categorias": [dict(r) for r in categorias_ag],
        "volume_mensal": [dict(r) for r in volume_mensal],
        "dias_semana": [dict(r) for r in dias_semana],
    }

    path = _snapshot_path()
    path.write_text(json.dumps(payload, ensure_ascii=False, default=_json_default), encoding="utf-8")
    return {"path": str(path), "count": len(items)}


def generate_snapshot_safe(limit: int = 2000) -> Dict[str, Any]:
    try:
        return {"ok": True, **generate_snapshot(limit=limit)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
