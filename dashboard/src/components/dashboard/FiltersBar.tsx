import { X, Filter, RotateCcw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { FiltrosState, NOMES_MESES, contarFiltrosAtivos } from '@/lib/data/filters';
import { formatarCategoria } from '@/lib/formatters';

interface FiltersBarProps {
  filtros: FiltrosState;
  onFiltroChange: (key: keyof FiltrosState, value: string | null) => void;
  onLimparFiltros: () => void;
  valoresUnicos: {
    anos: number[];
    meses: number[];
    categorias: string[];
    statusPagamento: string[];
    produtos: string[];
  };
}

export function FiltersBar({
  filtros,
  onFiltroChange,
  onLimparFiltros,
  valoresUnicos
}: FiltersBarProps) {
  const filtrosAtivos = contarFiltrosAtivos(filtros);

  return (
    <div className="dashboard-card-sm mb-6">
      <div className="flex items-center gap-4 flex-wrap">
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Filter className="w-4 h-4" />
          <span className="font-medium">Filtros</span>
          {filtrosAtivos > 0 && (
            <span className="filter-badge">
              {filtrosAtivos} ativo{filtrosAtivos > 1 ? 's' : ''}
            </span>
          )}
        </div>

        <div className="flex-1 flex items-center gap-3 flex-wrap">
          {/* Ano */}
          <Select
            value={filtros.ano?.toString() ?? 'todos'}
            onValueChange={(value) => onFiltroChange('ano', value === 'todos' ? null : value)}
          >
            <SelectTrigger className="w-[120px] bg-background">
              <SelectValue placeholder="Ano" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos os anos</SelectItem>
              {valoresUnicos.anos.map(ano => (
                <SelectItem key={ano} value={ano.toString()}>{ano}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Mês */}
          <Select
            value={filtros.mes?.toString() ?? 'todos'}
            onValueChange={(value) => onFiltroChange('mes', value === 'todos' ? null : value)}
          >
            <SelectTrigger className="w-[140px] bg-background">
              <SelectValue placeholder="Mês" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos os meses</SelectItem>
              {valoresUnicos.meses.map(mes => (
                <SelectItem key={mes} value={mes.toString()}>{NOMES_MESES[mes]}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Categoria */}
          <Select
            value={filtros.categoria ?? 'todos'}
            onValueChange={(value) => onFiltroChange('categoria', value === 'todos' ? null : value)}
          >
            <SelectTrigger className="w-[150px] bg-background">
              <SelectValue placeholder="Categoria" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todas as categorias</SelectItem>
              {valoresUnicos.categorias.map(cat => (
                <SelectItem key={cat} value={cat}>{formatarCategoria(cat)}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Status Pagamento */}
          <Select
            value={filtros.statusPagamento ?? 'todos'}
            onValueChange={(value) => onFiltroChange('statusPagamento', value === 'todos' ? null : value)}
          >
            <SelectTrigger className="w-[140px] bg-background">
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos os status</SelectItem>
              {valoresUnicos.statusPagamento.map(status => (
                <SelectItem key={status} value={status}>{status}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Produto */}
          <Select
            value={filtros.produto ?? 'todos'}
            onValueChange={(value) => onFiltroChange('produto', value === 'todos' ? null : value)}
          >
            <SelectTrigger className="w-[180px] bg-background">
              <SelectValue placeholder="Produto" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos os produtos</SelectItem>
              {valoresUnicos.produtos.map(prod => (
                <SelectItem key={prod} value={prod}>{prod}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Limpar Filtros */}
        {filtrosAtivos > 0 && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onLimparFiltros}
            className="text-muted-foreground hover:text-foreground"
          >
            <RotateCcw className="w-4 h-4 mr-1" />
            Limpar filtros
          </Button>
        )}
      </div>
    </div>
  );
}
