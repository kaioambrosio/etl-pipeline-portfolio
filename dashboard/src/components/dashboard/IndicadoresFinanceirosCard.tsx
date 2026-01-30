import { TrendingUp, CheckCircle, Clock } from 'lucide-react';
import { DashboardMetrics } from '@/lib/data/metrics';
import { formatarMoeda, formatarPercentual, formatarDias } from '@/lib/formatters';
import { LineChart, Line, ResponsiveContainer } from 'recharts';

interface IndicadoresFinanceirosCardProps {
  metricas: DashboardMetrics;
  sparklineData: number[];
}

export function IndicadoresFinanceirosCard({ metricas, sparklineData }: IndicadoresFinanceirosCardProps) {
  // Preparar dados para sparkline
  const sparklineChartData = sparklineData.map((valor, index) => ({ valor }));

  return (
    <div className="indicator-card dashboard-card h-full text-primary-foreground">
      <div className="relative z-10">
        <div className="flex items-center gap-2 mb-4">
          <TrendingUp className="w-5 h-5" />
          <h3 className="text-sm font-medium opacity-90">Indicadores Financeiros</h3>
        </div>

        {/* Ticket Médio - Destaque */}
        <div className="mb-6">
          <p className="text-xs opacity-75 mb-1">Ticket Médio</p>
          <p className="text-3xl font-bold">{formatarMoeda(metricas.ticketMedio)}</p>
        </div>

        {/* Badge % Pagas */}
        <div className="flex items-center gap-2 mb-6">
          <div className="flex items-center gap-1.5 bg-white/20 px-3 py-1.5 rounded-full">
            <CheckCircle className="w-4 h-4" />
            <span className="text-sm font-medium">{formatarPercentual(metricas.percentualPagas)} Pagas</span>
          </div>
        </div>

        {/* Sparkline */}
        <div className="mb-4">
          <p className="text-xs opacity-75 mb-2">Tendência de Volume</p>
          <div className="h-12">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={sparklineChartData}>
                <Line
                  type="monotone"
                  dataKey="valor"
                  stroke="rgba(255,255,255,0.9)"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Tempos */}
        <div className="pt-4 border-t border-white/20 space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 opacity-75" />
              <span className="text-xs opacity-75">Tempo de Processamento</span>
            </div>
            <span className="text-sm font-medium">{formatarDias(metricas.tempoMedioProcessamento)}</span>
          </div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Clock className="w-4 h-4 opacity-75" />
              <span className="text-xs opacity-75">Tempo até Pagamento</span>
            </div>
            <span className="text-sm font-medium">{formatarDias(metricas.tempoMedioPagamento)}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
