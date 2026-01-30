import { TrendingUp, TrendingDown, DollarSign, ShoppingCart, Activity } from "lucide-react";
import { DashboardMetrics, MetricasComparativas, DiaSemanaAgregado } from "@/lib/data/metrics";
import {
  formatarMoedaCompacta,
  formatarNumeroCompacto,
  formatarVariacao,
  formatarPercentual,
} from "@/lib/formatters";
import { BarChart, Bar, ResponsiveContainer, XAxis } from "recharts";
import { cn } from "@/lib/utils";

interface KpiCardsProps {
  metricas: DashboardMetrics;
  comparativas: MetricasComparativas;
  diasSemana: DiaSemanaAgregado[];
}

export function KpiCards({ metricas, comparativas, diasSemana }: KpiCardsProps) {
  const variacaoPositiva = comparativas.variacaoValor >= 0;
  const variacaoQtdPositiva = comparativas.variacaoQuantidade >= 0;
  const labelComparativo = comparativas.label ?? "vs período anterior";

  return (
    <div className="grid grid-cols-12 gap-6 mb-6">
      {/* Hero KPI - Valor Total Transacionado */}
      <div className="col-span-12 md:col-span-4">
        <div className="kpi-hero dashboard-card text-primary-foreground h-full">
          <div className="relative z-10">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-10 h-10 rounded-xl bg-white/20 flex items-center justify-center">
                <DollarSign className="w-5 h-5" />
              </div>
              <span className="text-sm font-medium opacity-90">
                Valor Total Transacionado
              </span>
            </div>

            <div className="text-4xl font-bold mb-3">
              {formatarMoedaCompacta(metricas.valorTotal)}
            </div>

            <div className="flex items-center gap-2">
              <div
                className={cn(
                  "flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium",
                  variacaoPositiva ? "bg-white/20" : "bg-white/10"
                )}
              >
                {variacaoPositiva ? (
                  <TrendingUp className="w-3 h-3" />
                ) : (
                  <TrendingDown className="w-3 h-3" />
                )}
                <span>{formatarVariacao(comparativas.variacaoValor)}</span>
              </div>
              <span className="text-xs opacity-75">{labelComparativo}</span>
            </div>
          </div>
        </div>
      </div>

      {/* KPI - Crescimento de Transações */}
      <div className="col-span-12 md:col-span-4">
        <div className="dashboard-card h-full">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-10 h-10 rounded-xl bg-primary-light flex items-center justify-center">
              <ShoppingCart className="w-5 h-5 text-primary" />
            </div>
            <span className="text-sm font-medium text-muted-foreground">
              Crescimento de Transações
            </span>
          </div>

          <div className="text-3xl font-bold text-foreground mb-2">
            {formatarNumeroCompacto(metricas.quantidadeTransacoes)}
          </div>

          <div className="flex items-center gap-2">
            <div
              className={cn(
                "flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium",
                variacaoQtdPositiva ? "bg-success-light text-success" : "bg-error-light text-error"
              )}
            >
              {variacaoQtdPositiva ? (
                <TrendingUp className="w-3 h-3" />
              ) : (
                <TrendingDown className="w-3 h-3" />
              )}
              <span>{formatarVariacao(comparativas.variacaoQuantidade)}</span>
            </div>
            <span className="text-xs text-muted-foreground">{labelComparativo}</span>
          </div>

          <div className="mt-4 pt-4 border-t border-border">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">Pagas</span>
              <span className="font-medium text-success">
                {formatarNumeroCompacto(metricas.quantidadePagas)}
              </span>
            </div>
            <div className="flex justify-between text-sm mt-1">
              <span className="text-muted-foreground">Taxa de Sucesso</span>
              <span className="font-medium">{formatarPercentual(metricas.percentualPagas)}</span>
            </div>
          </div>
        </div>
      </div>

      {/* KPI - Atividade Operacional */}
      <div className="col-span-12 md:col-span-4">
        <div className="dashboard-card h-full">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-10 h-10 rounded-xl bg-accent-light flex items-center justify-center">
              <Activity className="w-5 h-5 text-accent" />
            </div>
            <span className="text-sm font-medium text-muted-foreground">
              Atividade Operacional
            </span>
          </div>

          <div className="text-3xl font-bold text-foreground mb-2">
            {formatarNumeroCompacto(metricas.quantidadeTransacoes)}
            <span className="text-lg font-normal text-muted-foreground ml-1">transações</span>
          </div>

          {/* Mini Bar Chart por Dia da Semana */}
          <div className="mini-chart mt-4">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={diasSemana} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
                <XAxis
                  dataKey="dia"
                  axisLine={false}
                  tickLine={false}
                  tick={{ fontSize: 10, fill: "hsl(var(--muted-foreground))" }}
                  tickFormatter={(value) => value.substring(0, 3)}
                />
                <Bar dataKey="quantidade" fill="hsl(var(--accent))" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  );
}
