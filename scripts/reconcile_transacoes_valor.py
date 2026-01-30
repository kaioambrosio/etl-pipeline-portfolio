"""
Reconciliar valor total das transações com base nos itens.

Atualiza transacoes.valor usando a soma de transacao_itens.valor_total.
"""

import sys
from pathlib import Path

from sqlalchemy import text

# Adiciona o diretório raiz ao path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.models import get_engine


def main() -> None:
    engine = get_engine()
    update_sql = """
        UPDATE transacoes t
        SET valor = s.valor_total
        FROM (
            SELECT
                id_transacao,
                ROUND(SUM(valor_total)::numeric, 2) AS valor_total
            FROM transacao_itens
            GROUP BY id_transacao
        ) s
        WHERE t.id_transacao = s.id_transacao
          AND t.valor IS DISTINCT FROM s.valor_total
    """

    with engine.begin() as conn:
        result = conn.execute(text(update_sql))
        updated = result.rowcount

    print(f"Transacoes atualizadas: {updated}")


if __name__ == "__main__":
    main()
