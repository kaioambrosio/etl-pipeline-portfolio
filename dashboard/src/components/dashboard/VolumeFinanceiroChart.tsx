import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Area, AreaChart } from 'recharts';
import { VolumeTemporalMensal } from '@/lib/data/metrics';
import { formatarMoedaCompacta, formatarMoeda, formatarNumero } from '@/lib/formatters';

interface VolumeFinanceiroChartProps {
  dados: VolumeTemporalMensal[];
}

// Tooltip customizado
const CustomTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="bg-card border border-border rounded-lg shadow-lg p-3">
        <p className="font-medium text-foreground">{data.mes} {data.ano}</p>
        <p className="text-sm text-muted-foreground">
          Valor: <span className="font-medium text-primary">{formatarMoeda(data.valor)}</span>
        </p>
        <p className="text-sm text-muted-foreground">
          Transações: <span className="font-medium">{formatarNumero(data.quantidade)}</span>
        </p>
      </div>
    );
  }
  return null;
};

export function VolumeFinanceiroChart({ dados }: VolumeFinanceiroChartProps) {
  // Formatar dados para exibição
  const dadosFormatados = dados.map(d => ({
    ...d,
    label: `${d.mes}/${d.ano.toString().slice(-2)}`
  }));

  return (
    <div className="dashboard-card h-full">
      <div className="mb-4">
        <h3 className="text-base font-semibold text-foreground">Volume Financeiro</h3>
        <p className="text-sm text-muted-foreground">Evolução mensal</p>
      </div>

      <div className="h-[280px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={dadosFormatados}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <defs>
              <linearGradient id="colorValor" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid
              strokeDasharray="3 3"
              vertical={false}
              stroke="hsl(var(--border))"
            />
            <XAxis
              dataKey="label"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              interval="preserveStartEnd"
            />
            <YAxis
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickFormatter={(value) => formatarMoedaCompacta(value)}
              width={70}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="valor"
              stroke="hsl(var(--primary))"
              strokeWidth={2.5}
              fill="url(#colorValor)"
              dot={false}
              activeDot={{ r: 6, fill: 'hsl(var(--primary))', strokeWidth: 2, stroke: 'white' }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
