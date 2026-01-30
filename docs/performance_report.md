# ETL Pipeline - Relatório de Performance

## Resumo Executivo

O pipeline ETL foi testado com **5.000.000 transações** e **8.150.453 itens**, com **COPY nativo habilitado** para acelerar a carga. O tempo da etapa de carga caiu ~**71%** em relação ao baseline anterior.

## Ambiente de Teste

- **Sistema Operacional**: Windows 11
- **Python**: 3.12.6
- **PostgreSQL**: 18.1
- **Hardware**: Máquina local de desenvolvimento

## Configuração do ETL

- **ETL_USE_COPY**: `true`
- **ETL_COPY_THRESHOLD**: `200000`

## Resultados

### Métricas de Tempo (Transações 5M)

| Etapa | Tempo | % do Total |
|-------|-------|------------|
| Extração | 46s | 10,4% |
| Transformação | 46s | 10,4% |
| Carga (COPY) | 350,16s | 79,1% |
| **Total** | **442,97s (~7m23s)** | **100%** |

### Carga de Itens (transacao_itens)

- **Registros**: 8.150.453
- **Tempo**: ~284s (~4m44s)
- **Taxa**: ~28.699 itens/seg

### Pipeline completo

- **Arquivos processados**: 3 (catálogo, transações, itens)
- **Tempo total**: 730,34s (~12m10s)

### Taxa de Processamento (Transações)

- **Registros processados**: 5.000.000
- **Taxa geral (ETL transações)**: ~11.287 transações/seg
- **Taxa de carga (COPY)**: ~14.279 transações/seg
- **Taxa pipeline total (transações + itens + catálogo)**: ~18.006 registros/seg

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

1. **COPY nativo com ganho expressivo**: a carga caiu de ~1212s para **350s** (redução ~71%).
2. **ETL transações completo abaixo de 8 minutos** com 5M registros.
3. **Carga de itens eficiente** usando staging + COPY.

### Gargalos Identificados

1. **Carga ainda é a maior fatia do tempo**, mesmo com COPY (~79%).
   - Próximas otimizações possíveis:
     - Particionamento por data
     - Ajuste de `work_mem` e `maintenance_work_mem`
     - COPY direto em tabela particionada

## Conclusão

O pipeline ETL apresenta **performance robusta** para um portfólio com grande volume (5M transações + 8,1M itens), com melhorias claras após a adoção de COPY nativo.

---
*Relatório gerado em: 30/01/2026*
*Autor: Kaio Ambrosio*
