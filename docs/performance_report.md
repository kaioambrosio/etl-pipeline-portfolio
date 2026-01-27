# ETL Pipeline - Relatório de Performance

## Resumo Executivo

O pipeline ETL foi testado com **100.000 registros** para validar sua escalabilidade e performance.

## Ambiente de Teste

- **Sistema Operacional**: Windows 11
- **Python**: 3.12.6
- **PostgreSQL**: 17
- **Hardware**: Máquina local de desenvolvimento

## Resultados

### Métricas de Tempo

| Etapa | Tempo | % do Total |
|-------|-------|------------|
| Extração | ~0.5s | 2.2% |
| Transformação | ~1.0s | 4.5% |
| Carga (Load) | 21.65s | 93.3% |
| **Total** | **22.34s** | **100%** |

### Taxa de Processamento

- **Registros processados**: 100.000
- **Taxa geral**: ~4.476 registros/segundo
- **Taxa de carga**: ~4.619 registros/segundo

### Distribuição dos Dados

#### Por Status de Pagamento
| Status | Quantidade | Percentual | Valor Total |
|--------|------------|------------|-------------|
| CANCELADO | 39.978 | 40% | R$ 9.154.499.342 |
| PENDENTE | 39.818 | 40% | R$ 9.124.903.881 |
| PAGO | 20.204 | 20% | R$ 4.658.870.788 |

#### Por Categoria
| Categoria | Quantidade |
|-----------|------------|
| Acessórios | 20.244 |
| Eletrônicos | 20.118 |
| Periféricos | 19.911 |
| Componentes | 19.906 |
| Informática | 19.821 |

## Análise

### Pontos Fortes

1. **Extração Rápida**: A leitura do arquivo CSV de 7.78 MB foi realizada em menos de 1 segundo
2. **Transformação Eficiente**: O processamento de dados (limpeza, validação, transformação) foi extremamente rápido
3. **Escalabilidade**: O pipeline manteve performance linear com o aumento de dados

### Gargalos Identificados

1. **Carga no Banco**: 93% do tempo é gasto na inserção no PostgreSQL
   - Isso é esperado devido às operações de I/O
   - Pode ser otimizado com:
     - Bulk insert nativo (COPY)
     - Particionamento de tabelas
     - Conexões pooling

## Projeções

Com base nos testes realizados:

| Volume | Tempo Estimado |
|--------|----------------|
| 100.000 | 22s |
| 500.000 | ~2 min |
| 1.000.000 | ~4 min |

## Conclusão

O pipeline ETL demonstra **performance adequada** para o caso de uso proposto (portfólio e demonstração). A arquitetura é escalável e pode ser otimizada conforme necessário para volumes maiores de dados.

---
*Relatório gerado em: 26/01/2026*
*Autor: Kaio Ambrosio*
