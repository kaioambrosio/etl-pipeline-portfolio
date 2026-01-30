# ETL Pipeline - Relatório de Performance

## Resumo Executivo

O pipeline ETL foi testado com **5.000.000 transações** e **8.150.453 itens**, validando desempenho com grande volume de dados.

## Ambiente de Teste

- **Sistema Operacional**: Windows 11
- **Python**: 3.12.6
- **PostgreSQL**: 18.1
- **Hardware**: Máquina local de desenvolvimento

## Resultados

### Métricas de Tempo (Transações 5M)

| Etapa | Tempo | % do Total |
|-------|-------|------------|
| Extração | 25s | 2,0% |
| Transformação | 19s | 1,5% |
| Carga (Load) | 1211,96s | 96,5% |
| **Total** | **1255,57s (~20m56s)** | **100%** |

### Carga de Itens (transacao_itens)

- **Registros**: 8.150.453
- **Tempo**: ~756s (~12m36s)
- **Taxa**: ~10.781 itens/seg

### Pipeline completo

- **Arquivos processados**: 3 (catálogo, transações, itens)
- **Tempo total**: 2014,90s (~33m35s)

### Taxa de Processamento (Transações)

- **Registros processados**: 5.000.000
- **Taxa geral (ETL transações)**: ~3.982 transações/seg
- **Taxa de carga (transações)**: ~4.126 transações/seg

### Distribuição dos Dados

#### Por Status de Pagamento (transações)

| Status | Quantidade | Percentual | Valor Total |
|--------|------------|------------|-------------|
| PAGO | 3.029.360 | 60,6% | R$ 6.258.013.714,01 |
| PENDENTE | 929.366 | 18,6% | R$ 1.919.455.125,15 |
| ATRASADO | 487.526 | 9,8% | R$ 1.035.929.673,38 |
| CANCELADO | 359.933 | 7,2% | R$ 788.596.413,21 |
| ERRO | 193.815 | 3,9% | R$ 413.798.658,97 |

#### Por Categoria (transações)

| Categoria | Quantidade |
|-----------|------------|
| Componentes | 1.382.182 |
| Acessórios | 981.875 |
| Periféricos | 954.052 |
| Eletrônicos | 911.621 |
| Informática | 770.270 |

## Análise

### Pontos Fortes

1. **Extração e transformação rápidas**: Mesmo com 5M de registros, as etapas de leitura e transformação foram concluídas em menos de 1 minuto.
2. **Carga em lote eficiente**: O bulk insert manteve boa taxa de ingestão para o volume de dados.
3. **Escalabilidade**: O pipeline manteve comportamento estável em volume grande.

### Gargalos Identificados

1. **Carga no banco**: A etapa de inserção ainda domina o tempo total (~96%).
   - Possíveis otimizações futuras:
     - COPY nativo do PostgreSQL
     - Particionamento de tabelas por data
     - Ajustes de `work_mem` e `maintenance_work_mem`

## Conclusão

O pipeline ETL apresenta **performance sólida** para um portfólio com grande volume (5M transações + 8,1M itens), com resultados consistentes e métricas realistas para avaliação.

---
*Relatório gerado em: 30/01/2026*
*Autor: Kaio Ambrosio*
