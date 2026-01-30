import { Search, ChevronLeft, ChevronRight, Info } from "lucide-react";
import { Fragment, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { Transacao } from "@/lib/data/mockGenerator";
import { formatarCategoria, formatarMoeda, formatarData } from "@/lib/formatters";
import { fetchApiTransacaoDetalhe } from "@/lib/data/api";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

interface TransacoesTableProps {
  transacoes: Transacao[];
  total: number | null;
  totalLoading?: boolean;
  paginaAtual: number;
  itensPorPagina: number;
  busca: string;
  onBuscaChange: (value: string) => void;
  onPaginaChange: (pagina: number) => void;
  detalheDisponivel?: boolean;
}

export function TransacoesTable({
  transacoes,
  total,
  totalLoading = false,
  paginaAtual,
  itensPorPagina,
  busca,
  onBuscaChange,
  onPaginaChange,
  detalheDisponivel = false,
}: TransacoesTableProps) {
  const totalDisponivel = total !== null && total !== undefined;
  const totalPaginas = totalDisponivel ? Math.ceil(total / itensPorPagina) : 0;
  const inicio = (paginaAtual - 1) * itensPorPagina;
  const fim = totalDisponivel ? Math.min(inicio + itensPorPagina, total) : inicio + transacoes.length;
  const [transacaoSelecionada, setTransacaoSelecionada] = useState<string | null>(null);

  const getStatusClass = (status: string) => {
    switch (status) {
      case "Pago":
        return "status-pago";
      case "Pendente":
        return "status-pendente";
      case "Atrasado":
        return "status-atrasado";
      case "Cancelado":
        return "status-cancelado";
      case "Erro":
        return "status-erro";
      default:
        return "status-erro";
    }
  };

  const detalheQuery = useQuery({
    queryKey: ["api", "transacao-detalhe", transacaoSelecionada],
    queryFn: () => fetchApiTransacaoDetalhe(transacaoSelecionada || ""),
    enabled: detalheDisponivel && !!transacaoSelecionada,
  });

  const detalhe = detalheQuery.data?.transacao;
  const itens = detalheQuery.data?.itens ?? [];

  return (
    <div className="dashboard-card">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-base font-semibold text-foreground">Transações Recentes</h3>
          <p className="text-sm text-muted-foreground">
            {totalDisponivel
              ? `${total} transações encontradas`
              : totalLoading
              ? "Calculando total..."
              : "Total em atualização"}
          </p>
        </div>

        {/* Busca */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <Input
            type="text"
            placeholder="Buscar transação..."
            value={busca}
            onChange={(e) => {
              onBuscaChange(e.target.value);
            }}
            className="pl-9 w-64 bg-background"
          />
        </div>
      </div>

      {totalDisponivel && total === 0 ? (
        <div className="py-12 text-center">
          <p className="text-muted-foreground">
            Nenhuma transação encontrada com os filtros atuais.
          </p>
        </div>
      ) : (
        <>
          <div className="rounded-lg border border-border overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow className="bg-muted/50">
                  <TableHead className="font-semibold">Data</TableHead>
                  <TableHead className="font-semibold">Cliente</TableHead>
                  <TableHead className="font-semibold">Produto</TableHead>
                  <TableHead className="font-semibold">Categoria</TableHead>
                  <TableHead className="font-semibold text-right">Valor</TableHead>
                  <TableHead className="font-semibold text-center">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {transacoes.map((transacao) => {
                  const isOpen = transacaoSelecionada === transacao.id_transacao;
                  return (
                    <Fragment key={transacao.id_transacao}>
                      <TableRow
                        className="hover:bg-muted/30 cursor-pointer"
                        onClick={() =>
                          setTransacaoSelecionada(isOpen ? null : transacao.id_transacao)
                        }
                      >
                        <TableCell className="text-muted-foreground">
                          {formatarData(transacao.data_transacao)}
                        </TableCell>
                        <TableCell className="font-medium">{transacao.cliente}</TableCell>
                        <TableCell className="text-muted-foreground">
                          {transacao.produto}
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          {formatarCategoria(transacao.categoria)}
                        </TableCell>
                        <TableCell className="text-right font-medium">
                          {formatarMoeda(transacao.valor)}
                        </TableCell>
                        <TableCell className="text-center">
                          <span className={cn("status-pill", getStatusClass(transacao.status_pagamento))}>
                            {transacao.status_pagamento}
                          </span>
                        </TableCell>
                      </TableRow>
                      {isOpen && (
                        <TableRow className="bg-muted/20">
                          <TableCell colSpan={6}>
                            <div className="p-4 rounded-lg border border-border bg-background">
                              <div className="flex items-center gap-2 mb-3">
                                <Info className="w-4 h-4 text-muted-foreground" />
                                <span className="text-sm font-semibold">
                                  Detalhes da transação
                                </span>
                              </div>

                              {!detalheDisponivel ? (
                                <div className="text-sm text-muted-foreground">
                                  Detalhes disponíveis somente com a API ativa.
                                </div>
                              ) : detalheQuery.isLoading ? (
                                <div className="text-sm text-muted-foreground">
                                  Carregando detalhes...
                                </div>
                              ) : !detalhe ? (
                                <div className="text-sm text-muted-foreground">
                                  Detalhes não encontrados.
                                </div>
                              ) : (
                                <div className="space-y-3">
                                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                                    <div>
                                      <p className="text-muted-foreground">Cliente</p>
                                      <p className="font-medium">{detalhe.cliente}</p>
                                    </div>
                                    <div>
                                      <p className="text-muted-foreground">Produto principal</p>
                                      <p className="font-medium">{detalhe.produto}</p>
                                    </div>
                                    <div>
                                      <p className="text-muted-foreground">Categoria</p>
                                      <p className="font-medium">{formatarCategoria(detalhe.categoria)}</p>
                                    </div>
                                    <div>
                                      <p className="text-muted-foreground">Status</p>
                                      <p className="font-medium">{detalhe.status_pagamento}</p>
                                    </div>
                                    <div>
                                      <p className="text-muted-foreground">Valor total</p>
                                      <p className="font-medium">{formatarMoeda(detalhe.valor)}</p>
                                    </div>
                                    <div>
                                      <p className="text-muted-foreground">Pagamento</p>
                                      <p className="font-medium">
                                        {detalhe.data_pagamento
                                          ? formatarData(detalhe.data_pagamento)
                                          : "Não pago"}
                                      </p>
                                    </div>
                                  </div>

                                  <div className="rounded-md border border-border overflow-hidden">
                                    <table className="w-full text-sm">
                                      <thead className="bg-muted/50">
                                        <tr>
                                          <th className="text-left font-semibold px-3 py-2">Produto</th>
                                          <th className="text-left font-semibold px-3 py-2">Descrição</th>
                                          <th className="text-right font-semibold px-3 py-2">Qtd</th>
                                          <th className="text-right font-semibold px-3 py-2">Unitário</th>
                                          <th className="text-right font-semibold px-3 py-2">Total</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {itens.map((item, idx) => (
                                          <tr key={`${item.produto}-${idx}`} className="border-t">
                                            <td className="px-3 py-2">{item.produto}</td>
                                            <td className="px-3 py-2 text-muted-foreground">
                                              {item.produto_descricao || "-"}
                                            </td>
                                            <td className="px-3 py-2 text-right">{item.quantidade}</td>
                                            <td className="px-3 py-2 text-right">
                                              {formatarMoeda(item.valor_unitario)}
                                            </td>
                                            <td className="px-3 py-2 text-right">
                                              {formatarMoeda(item.valor_total)}
                                            </td>
                                          </tr>
                                        ))}
                                      </tbody>
                                    </table>
                                  </div>
                                </div>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })}
              </TableBody>
            </Table>
          </div>

          {/* Paginação */}
          {totalDisponivel && totalPaginas > 1 && (
            <div className="flex items-center justify-between mt-4 pt-4 border-t border-border">
              <p className="text-sm text-muted-foreground">
                Mostrando {inicio + 1}-{fim} de {total}
              </p>

              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => onPaginaChange(Math.max(1, paginaAtual - 1))}
                  disabled={paginaAtual === 1}
                >
                  <ChevronLeft className="w-4 h-4" />
                </Button>

                <div className="flex items-center gap-1">
                  {Array.from({ length: Math.min(5, totalPaginas) }, (_, i) => {
                    let pageNum;
                    if (totalPaginas <= 5) {
                      pageNum = i + 1;
                    } else if (paginaAtual <= 3) {
                      pageNum = i + 1;
                    } else if (paginaAtual >= totalPaginas - 2) {
                      pageNum = totalPaginas - 4 + i;
                    } else {
                      pageNum = paginaAtual - 2 + i;
                    }

                    return (
                      <Button
                        key={pageNum}
                        variant={paginaAtual === pageNum ? "default" : "ghost"}
                        size="sm"
                        onClick={() => onPaginaChange(pageNum)}
                        className="w-8 h-8 p-0"
                      >
                        {pageNum}
                      </Button>
                    );
                  })}
                </div>

                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => onPaginaChange(Math.min(totalPaginas, paginaAtual + 1))}
                  disabled={paginaAtual === totalPaginas}
                >
                  <ChevronRight className="w-4 h-4" />
                </Button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
