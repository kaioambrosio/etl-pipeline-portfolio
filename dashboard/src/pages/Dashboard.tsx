import { useState, useMemo, useCallback, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";

import { Sidebar } from "@/components/dashboard/Sidebar";
import { Topbar } from "@/components/dashboard/Topbar";
import { FiltersBar } from "@/components/dashboard/FiltersBar";
import { KpiCards } from "@/components/dashboard/KpiCards";
import { DistribuicaoCategoriaChart } from "@/components/dashboard/DistribuicaoCategoriaChart";
import { VolumeFinanceiroChart } from "@/components/dashboard/VolumeFinanceiroChart";
import { IndicadoresFinanceirosCard } from "@/components/dashboard/IndicadoresFinanceirosCard";
import { TransacoesTable } from "@/components/dashboard/TransacoesTable";
import { EmptyState } from "@/components/dashboard/EmptyState";

import {
  fetchApiCategorias,
  fetchApiComparativo,
  fetchApiDiasSemana,
  fetchApiFiltros,
  fetchApiMetricas,
  fetchApiTransacoes,
  fetchApiTransacoesTotal,
  fetchApiVolumeMensal,
  fetchSnapshot,
  normalizeSnapshot,
  USE_MOCK,
} from "@/lib/data/api";
import {
  calcularMetricas,
  calcularComparativoPorPeriodo,
  agregarPorCategoria,
  calcularVolumeTemporal,
  agregarPorDiaSemana,
  gerarSparklineData,
  gerarSparklineDataDeVolume,
} from "@/lib/data/metrics";
import {
  FiltrosState,
  filtrosIniciais,
  aplicarFiltros,
  obterValoresUnicos,
  temFiltrosAtivos,
} from "@/lib/data/filters";

const ITEMS_POR_PAGINA = 20;

function resolvePeriodoComparativo(
  volumeMensal: { ano: number; mesNumero: number }[],
  ano?: number | null,
  mes?: number | null
) {
  if (ano && mes) {
    return { tipo: "mes" as const, ano, mes, label: "vs mês anterior" };
  }
  if (ano && !mes) {
    return { tipo: "ano" as const, ano, label: "vs ano anterior" };
  }
  if (mes && !ano) {
    const maxAno = Math.max(
      ...volumeMensal.filter((v) => v.mesNumero === mes).map((v) => v.ano),
      -Infinity
    );
    if (Number.isFinite(maxAno)) {
      return { tipo: "mes" as const, ano: maxAno, mes, label: "vs mês anterior" };
    }
  }
  const ultimo = volumeMensal[volumeMensal.length - 1];
  if (ultimo) {
    return {
      tipo: "mes" as const,
      ano: ultimo.ano,
      mes: ultimo.mesNumero,
      label: "vs mês anterior",
    };
  }
  return null;
}

export default function Dashboard() {
  const [filtros, setFiltros] = useState<FiltrosState>(filtrosIniciais);
  const [categoriaChart, setCategoriaChart] = useState<string | null>(null);
  const [paginaAtual, setPaginaAtual] = useState(1);
  const [busca, setBusca] = useState("");
  const [buscaDebounced, setBuscaDebounced] = useState("");

  const apiEnabled = !USE_MOCK;

  const categoriaCross = categoriaChart ?? filtros.categoria;
  const filtrosBase = useMemo(
    () => ({
      ano: filtros.ano,
      mes: filtros.mes,
      categoria: filtros.categoria,
      statusPagamento: filtros.statusPagamento,
      produto: filtros.produto,
    }),
    [filtros.ano, filtros.mes, filtros.categoria, filtros.statusPagamento, filtros.produto]
  );

  const filtrosCross = useMemo(
    () => ({
      ...filtrosBase,
      categoria: categoriaCross,
      busca: buscaDebounced,
    }),
    [filtrosBase, categoriaCross, buscaDebounced]
  );

  const filtrosKey = [
    filtros.ano,
    filtros.mes,
    filtros.categoria,
    filtros.statusPagamento,
    filtros.produto,
  ];
  const filtrosCrossKey = [
    filtros.ano,
    filtros.mes,
    categoriaCross,
    filtros.statusPagamento,
    filtros.produto,
  ];
  const transacoesKey = [
    ...filtrosCrossKey,
    buscaDebounced,
    paginaAtual,
  ];
  const totalKey = [...filtrosCrossKey, buscaDebounced];

  const filtrosQuery = useQuery({
    queryKey: ["api", "filtros", ...filtrosKey],
    queryFn: () => fetchApiFiltros(filtrosBase),
    enabled: apiEnabled,
  });

  const metricasQuery = useQuery({
    queryKey: ["api", "metricas", ...filtrosKey],
    queryFn: () => fetchApiMetricas(filtrosBase),
    enabled: apiEnabled,
  });

  const comparativoQuery = useQuery({
    queryKey: ["api", "comparativo", ...filtrosKey],
    queryFn: () => fetchApiComparativo(filtrosBase),
    enabled: apiEnabled,
  });

  const categoriasQuery = useQuery({
    queryKey: ["api", "categorias", ...filtrosKey],
    queryFn: () => fetchApiCategorias(filtrosBase),
    enabled: apiEnabled,
  });

  const volumeBaseQuery = useQuery({
    queryKey: ["api", "volume", ...filtrosKey],
    queryFn: () => fetchApiVolumeMensal(filtrosBase),
    enabled: apiEnabled,
  });

  const volumeCrossQuery = useQuery({
    queryKey: ["api", "volume-cross", ...filtrosCrossKey],
    queryFn: () => fetchApiVolumeMensal({ ...filtrosBase, categoria: categoriaCross }),
    enabled: apiEnabled && !!categoriaChart,
  });

  const diasSemanaQuery = useQuery({
    queryKey: ["api", "dias-semana", ...filtrosKey],
    queryFn: () => fetchApiDiasSemana(filtrosBase),
    enabled: apiEnabled,
  });

  const transacoesQuery = useQuery({
    queryKey: ["api", "transacoes", ...transacoesKey],
    queryFn: () =>
      fetchApiTransacoes(
        filtrosCross,
        ITEMS_POR_PAGINA,
        (paginaAtual - 1) * ITEMS_POR_PAGINA,
        false
      ),
    enabled: apiEnabled,
    placeholderData: (previous) => previous,
  });

  const totalQuery = useQuery({
    queryKey: ["api", "transacoes-total", ...totalKey],
    queryFn: () => fetchApiTransacoesTotal(filtrosCross),
    enabled: apiEnabled,
    placeholderData: (previous) => previous,
  });

  const apiQueries = [
    filtrosQuery,
    metricasQuery,
    comparativoQuery,
    categoriasQuery,
    volumeBaseQuery,
    diasSemanaQuery,
    transacoesQuery,
  ];

  const apiLoading = apiEnabled && apiQueries.some((query) => query.isLoading);
  const apiError = apiEnabled && apiQueries.some((query) => query.isError);

  const fallbackEnabled = !apiEnabled || apiError;

  const snapshotQuery = useQuery({
    queryKey: ["snapshot"],
    queryFn: fetchSnapshot,
    enabled: fallbackEnabled,
  });

  const fallbackSnapshot = useMemo(() => {
    if (!fallbackEnabled) return null;
    return normalizeSnapshot(snapshotQuery.data);
  }, [fallbackEnabled, snapshotQuery.data]);

  const fallbackBase = fallbackSnapshot?.transacoes ?? [];

  const fallbackData = useMemo(() => {
    if (!fallbackEnabled) return null;

    const dadosFiltrados = aplicarFiltros(fallbackBase, filtros);
    const dadosBusca = buscaDebounced.trim()
      ? dadosFiltrados.filter((t) => {
          const termo = buscaDebounced.toLowerCase();
          return (
            t.cliente.toLowerCase().includes(termo) ||
            t.produto.toLowerCase().includes(termo) ||
            t.categoria.toLowerCase().includes(termo)
          );
        })
      : dadosFiltrados;

    const dadosComCross = categoriaChart
      ? dadosBusca.filter((t) => t.categoria === categoriaChart)
      : dadosBusca;

    const volumeBase = calcularVolumeTemporal(dadosFiltrados);
    const volumeCross = categoriaChart
      ? calcularVolumeTemporal(dadosComCross)
      : volumeBase;
    const periodoFallback = resolvePeriodoComparativo(
      volumeBase,
      filtros.ano,
      filtros.mes
    );

    const usarSnapshotAgregado =
      !temFiltrosAtivos(filtros) && !buscaDebounced.trim() && !categoriaChart;

    const total = dadosComCross.length;
    const inicio = (paginaAtual - 1) * ITEMS_POR_PAGINA;
    const transacoesPaginadas = dadosComCross.slice(
      inicio,
      inicio + ITEMS_POR_PAGINA
    );

    return {
      valoresUnicos:
        fallbackSnapshot?.filtros ?? obterValoresUnicos(fallbackBase),
      metricas:
        usarSnapshotAgregado && fallbackSnapshot?.metricas
          ? fallbackSnapshot.metricas
          : calcularMetricas(dadosFiltrados),
      comparativas: periodoFallback
        ? calcularComparativoPorPeriodo(
            volumeBase,
            periodoFallback,
            periodoFallback.label
          )
        : {
            valorTotalAnterior: 0,
            quantidadeAnterior: 0,
            variacaoValor: 0,
            variacaoQuantidade: 0,
            label: "vs período anterior",
          },
      categorias:
        usarSnapshotAgregado && fallbackSnapshot?.categorias
          ? fallbackSnapshot.categorias
          : agregarPorCategoria(dadosFiltrados),
      volumeTemporal: volumeCross,
      volumeBase,
      diasSemana:
        usarSnapshotAgregado && fallbackSnapshot?.diasSemana
          ? fallbackSnapshot.diasSemana
          : agregarPorDiaSemana(dadosFiltrados),
      sparklineData: gerarSparklineData(dadosFiltrados),
      transacoes: transacoesPaginadas,
      total,
    };
  }, [
    fallbackEnabled,
    fallbackBase,
    fallbackSnapshot,
    filtros,
    buscaDebounced,
    categoriaChart,
    paginaAtual,
  ]);

  const volumeBase = volumeBaseQuery.data ?? [];
  const volumeCross =
    categoriaChart && volumeCrossQuery.data ? volumeCrossQuery.data : volumeBase;
  const periodoComparativoApi = resolvePeriodoComparativo(
    volumeBase,
    filtros.ano,
    filtros.mes
  );
  const comparativoFallbackApi = periodoComparativoApi
    ? calcularComparativoPorPeriodo(
        volumeBase,
        periodoComparativoApi,
        periodoComparativoApi.label
      )
    : {
        valorTotalAnterior: 0,
        quantidadeAnterior: 0,
        variacaoValor: 0,
        variacaoQuantidade: 0,
        label: "vs período anterior",
      };

  const metricas = fallbackEnabled
    ? fallbackData?.metricas
    : metricasQuery.data;

  const comparativas = fallbackEnabled
    ? fallbackData?.comparativas
    : comparativoQuery.data ?? comparativoFallbackApi;

  const categorias = fallbackEnabled
    ? fallbackData?.categorias
    : categoriasQuery.data;

  const diasSemana = fallbackEnabled
    ? fallbackData?.diasSemana
    : diasSemanaQuery.data;

  const sparklineData = fallbackEnabled
    ? fallbackData?.sparklineData
    : gerarSparklineDataDeVolume(volumeBase);

  const valoresUnicos = fallbackEnabled
    ? fallbackData?.valoresUnicos
    : filtrosQuery.data;

  const transacoesTabela = fallbackEnabled
    ? fallbackData?.transacoes
    : transacoesQuery.data?.items;

  const totalTransacoes = fallbackEnabled
    ? fallbackData?.total ?? 0
    : totalQuery.data?.total ?? null;
  const totalLoading = !fallbackEnabled && totalQuery.isLoading;

  const volumeTemporal = fallbackEnabled
    ? fallbackData?.volumeTemporal
    : volumeCross;

  const isLoading = fallbackEnabled ? snapshotQuery.isLoading : apiLoading;
  const totalBase = metricas?.quantidadeTransacoes ?? 0;
  const temDados = totalBase > 0;

  const handleFiltroChange = useCallback(
    (key: keyof FiltrosState, value: string | null) => {
      setFiltros((prev) => ({
        ...prev,
        [key]:
          key === "ano" || key === "mes" ? (value ? parseInt(value) : null) : value,
      }));
      setCategoriaChart(null);
      setPaginaAtual(1);
    },
    []
  );

  const handleLimparFiltros = useCallback(() => {
    setFiltros(filtrosIniciais);
    setCategoriaChart(null);
    setPaginaAtual(1);
  }, []);

  const handleCategoriaClick = useCallback((categoria: string | null) => {
    setCategoriaChart(categoria);
    setPaginaAtual(1);
  }, []);

  const handleBuscaChange = useCallback((value: string) => {
    setBusca(value);
    setPaginaAtual(1);
  }, []);

  useEffect(() => {
    const handle = setTimeout(() => {
      setBuscaDebounced(busca);
    }, 300);
    return () => clearTimeout(handle);
  }, [busca]);

  const handlePaginaChange = useCallback((pagina: number) => {
    setPaginaAtual(pagina);
  }, []);

  return (
    <div className="min-h-screen bg-background flex">
      <Sidebar />

      <div className="flex-1 ml-16">
        <Topbar />

        <main className="p-6">
          <FiltersBar
            filtros={filtros}
            onFiltroChange={handleFiltroChange}
            onLimparFiltros={handleLimparFiltros}
            valoresUnicos={
              valoresUnicos ?? {
                anos: [],
                meses: [],
                categorias: [],
                statusPagamento: [],
                produtos: [],
              }
            }
          />

          {isLoading ? (
            <div className="p-6 text-muted-foreground">Carregando dados...</div>
          ) : !temDados ? (
            <EmptyState
              onLimparFiltros={
                temFiltrosAtivos(filtros) ? handleLimparFiltros : undefined
              }
            />
          ) : (
            <>
              <KpiCards
                metricas={
                  metricas ?? {
                    valorTotal: 0,
                    quantidadeTransacoes: 0,
                    ticketMedio: 0,
                    quantidadePagas: 0,
                    percentualPagas: 0,
                    tempoMedioProcessamento: 0,
                    tempoMedioPagamento: 0,
                  }
                }
                comparativas={
                  comparativas ?? {
                    valorTotalAnterior: 0,
                    quantidadeAnterior: 0,
                    variacaoValor: 0,
                    variacaoQuantidade: 0,
                  }
                }
                diasSemana={diasSemana ?? []}
              />

              <div className="grid grid-cols-12 gap-6 mb-6">
                <div className="col-span-12 lg:col-span-4">
                  <DistribuicaoCategoriaChart
                    dados={categorias ?? []}
                    categoriaAtiva={categoriaChart}
                    onCategoriaClick={handleCategoriaClick}
                  />
                </div>

                <div className="col-span-12 lg:col-span-5">
                  <VolumeFinanceiroChart dados={volumeTemporal ?? []} />
                </div>

                <div className="col-span-12 lg:col-span-3">
                  <IndicadoresFinanceirosCard
                    metricas={
                      metricas ?? {
                        valorTotal: 0,
                        quantidadeTransacoes: 0,
                        ticketMedio: 0,
                        quantidadePagas: 0,
                        percentualPagas: 0,
                        tempoMedioProcessamento: 0,
                        tempoMedioPagamento: 0,
                      }
                    }
                    sparklineData={sparklineData ?? []}
                  />
                </div>
              </div>

              <TransacoesTable
                transacoes={transacoesTabela ?? []}
                total={totalTransacoes}
                totalLoading={totalLoading}
                paginaAtual={paginaAtual}
                itensPorPagina={ITEMS_POR_PAGINA}
                busca={busca}
                onBuscaChange={handleBuscaChange}
                onPaginaChange={handlePaginaChange}
                detalheDisponivel={!fallbackEnabled}
              />
            </>
          )}
        </main>
      </div>
    </div>
  );
}
