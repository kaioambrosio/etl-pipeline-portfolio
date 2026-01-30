import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { CategoriaAgregada } from '@/lib/data/metrics';
import { formatarCategoria, formatarMoedaCompacta, formatarMoeda } from '@/lib/formatters';

interface DistribuicaoCategoriaChartProps {
  dados: CategoriaAgregada[];
  categoriaAtiva: string | null;
  onCategoriaClick: (categoria: string | null) => void;
}

const CORES = [
  'hsl(var(--chart-1))',
  'hsl(var(--chart-2))',
  'hsl(var(--chart-3))',
  'hsl(var(--chart-4))',
  'hsl(var(--chart-5))',
  'hsl(var(--chart-6))',
  'hsl(var(--chart-7))',
  'hsl(var(--chart-8))',
];

// Tooltip customizado
const CustomTooltip = ({ active, payload }: any) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="bg-card border border-border rounded-lg shadow-lg p-3">
        <p className="font-medium text-foreground">{formatarCategoria(data.categoria)}</p>
        <p className="text-sm text-muted-foreground">
          Valor: <span className="font-medium text-primary">{formatarMoeda(data.valor)}</span>
        </p>
        <p className="text-sm text-muted-foreground">
          Quantidade: <span className="font-medium">{data.quantidade}</span>
        </p>
      </div>
    );
  }
  return null;
};

export function DistribuicaoCategoriaChart({
  dados,
  categoriaAtiva,
  onCategoriaClick
}: DistribuicaoCategoriaChartProps) {
  const handleClick = (data: any) => {
    if (categoriaAtiva === data.categoria) {
      onCategoriaClick(null);
    } else {
      onCategoriaClick(data.categoria);
    }
  };

  return (
    <div className="dashboard-card h-full">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-base font-semibold text-foreground">Distribuição Financeira</h3>
          <p className="text-sm text-muted-foreground">Por categoria</p>
        </div>
        {categoriaAtiva && (
          <button
            onClick={() => onCategoriaClick(null)}
            className="text-xs text-primary hover:underline"
          >
            Limpar seleção
          </button>
        )}
      </div>

      <div className="h-[280px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={dados}
            layout="vertical"
            margin={{ top: 0, right: 16, left: 0, bottom: 0 }}
          >
            <XAxis
              type="number"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickFormatter={(value) => formatarMoedaCompacta(value)}
            />
            <YAxis
              type="category"
              dataKey="categoria"
              axisLine={false}
              tickLine={false}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickFormatter={(value) => formatarCategoria(value)}
              width={80}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'hsl(var(--muted) / 0.5)' }} />
            <Bar
              dataKey="valor"
              radius={[0, 6, 6, 0]}
              cursor="pointer"
              onClick={(data) => handleClick(data)}
            >
              {dados.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={CORES[index % CORES.length]}
                  opacity={categoriaAtiva === null || categoriaAtiva === entry.categoria ? 1 : 0.3}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
