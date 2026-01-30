import { FileX } from 'lucide-react';
import { Button } from '@/components/ui/button';

interface EmptyStateProps {
  titulo?: string;
  mensagem?: string;
  onLimparFiltros?: () => void;
}

export function EmptyState({
  titulo = 'Sem dados para exibir',
  mensagem = 'Nenhum dado encontrado com os filtros selecionados.',
  onLimparFiltros
}: EmptyStateProps) {
  return (
    <div className="dashboard-card flex flex-col items-center justify-center py-16">
      <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center mb-4">
        <FileX className="w-8 h-8 text-muted-foreground" />
      </div>

      <h3 className="text-lg font-semibold text-foreground mb-2">{titulo}</h3>
      <p className="text-sm text-muted-foreground text-center max-w-md mb-6">
        {mensagem}
      </p>

      {onLimparFiltros && (
        <Button onClick={onLimparFiltros} variant="outline">
          Limpar filtros e tentar novamente
        </Button>
      )}
    </div>
  );
}
