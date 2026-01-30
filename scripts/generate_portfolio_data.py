import argparse
import csv
import random
import sys
from bisect import bisect
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence, TypeVar

from faker import Faker

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


CATEGORIAS = {
    "Acessorios": "Acessórios e complementos para dispositivos.",
    "Componentes": "Componentes internos e upgrades de hardware.",
    "Eletronicos": "Dispositivos eletrônicos de uso pessoal.",
    "Informatica": "Equipamentos de TI e infraestrutura.",
    "Perifericos": "Periféricos de entrada, áudio e vídeo.",
}


CATALOGO = [
    # Acessorios (15)
    ("Acessorios", "Carregador USB-C 65W", "Carregador rápido USB-C para notebooks e celulares.", 90, 260, 8),
    ("Acessorios", "Cabo HDMI 2.1", "Cabo HDMI de alta velocidade para 4K.", 30, 120, 7),
    ("Acessorios", "Mousepad Gamer", "Mousepad com superfície de baixa fricção.", 40, 140, 6),
    ("Acessorios", "Hub USB 3.0", "Hub USB com 4 portas e alta velocidade.", 80, 220, 6),
    ("Acessorios", "Filtro de Linha", "Filtro de linha com proteção contra surtos.", 70, 180, 5),
    ("Acessorios", "Adaptador Bluetooth", "Adaptador USB Bluetooth 5.0.", 60, 160, 5),
    ("Acessorios", "Suporte para Notebook", "Suporte ergonômico ajustável.", 80, 220, 4),
    ("Acessorios", "Base Cooler", "Base refrigerada para notebook.", 120, 320, 4),
    ("Acessorios", "Cabo Ethernet Cat6", "Cabo de rede blindado Cat6.", 25, 90, 4),
    ("Acessorios", "Capa para Notebook", "Capa protetora acolchoada.", 60, 200, 4),
    ("Acessorios", "Dongle Wi-Fi USB", "Adaptador Wi-Fi USB dual band.", 80, 230, 4),
    ("Acessorios", "Leitor de Cartões", "Leitor USB para SD e microSD.", 40, 120, 3),
    ("Acessorios", "Cartão SD 128GB", "Cartão SD classe 10 128GB.", 90, 220, 3),
    ("Acessorios", "Organizador de Cabos", "Kit com presilhas e velcros.", 25, 90, 3),
    ("Acessorios", "Capa para Smartphone", "Capa resistente a impactos.", 40, 160, 4),
    # Componentes (15)
    ("Componentes", "Processador Ryzen 5", "Processador 6 cores para desktop.", 850, 1400, 9),
    ("Componentes", "Processador Intel i5", "Processador Intel para uso geral.", 900, 1500, 8),
    ("Componentes", "Placa de Vídeo RTX 4060", "GPU para jogos em 1080p/1440p.", 1800, 3200, 9),
    ("Componentes", "Memória RAM 16GB", "Módulo DDR4 16GB 3200MHz.", 280, 520, 10),
    ("Componentes", "Memória RAM 32GB", "Módulo DDR4 32GB 3200MHz.", 520, 980, 6),
    ("Componentes", "SSD NVMe 1TB", "SSD NVMe de alta performance.", 380, 720, 9),
    ("Componentes", "SSD SATA 512GB", "SSD SATA confiável para upgrades.", 220, 420, 7),
    ("Componentes", "Fonte 650W", "Fonte 80 Plus Bronze.", 350, 700, 7),
    ("Componentes", "Gabinete Mid Tower", "Gabinete com airflow otimizado.", 280, 650, 6),
    ("Componentes", "Placa Mãe B550", "Placa mãe para Ryzen AM4.", 650, 1100, 6),
    ("Componentes", "HD 2TB", "HD interno 7200rpm.", 320, 560, 5),
    ("Componentes", "Pasta Térmica", "Pasta térmica de alta condutividade.", 40, 140, 4),
    ("Componentes", "Fan RGB 120mm", "Ventoinha RGB 120mm.", 60, 180, 5),
    ("Componentes", "Water Cooler 240mm", "Refrigeração líquida 240mm.", 520, 1100, 4),
    ("Componentes", "Placa de Captura", "Placa para captura de vídeo.", 420, 900, 3),
    # Eletronicos (15)
    ("Eletronicos", "Smartphone 128GB", "Smartphone 128GB com câmera tripla.", 1200, 2800, 9),
    ("Eletronicos", "Smartphone 256GB", "Smartphone 256GB premium.", 2000, 4200, 6),
    ("Eletronicos", "Tablet 10\"", "Tablet 10 polegadas para multimídia.", 900, 2100, 7),
    ("Eletronicos", "Smartwatch", "Relógio inteligente com GPS.", 500, 1400, 6),
    ("Eletronicos", "Fone Bluetooth", "Fone sem fio com cancelamento.", 220, 820, 8),
    ("Eletronicos", "Câmera de Ação", "Câmera resistente para esportes.", 900, 2400, 3),
    ("Eletronicos", "TV Box", "Box de streaming 4K.", 180, 600, 5),
    ("Eletronicos", "Caixa de Som Bluetooth", "Caixa portátil com bateria longa.", 180, 700, 6),
    ("Eletronicos", "Console Portátil", "Console compacto para jogos.", 900, 2200, 3),
    ("Eletronicos", "E-reader", "Leitor digital com tela e-ink.", 500, 1200, 3),
    ("Eletronicos", "Câmera Instantânea", "Câmera com impressão instantânea.", 450, 1200, 2),
    ("Eletronicos", "Drone Compacto", "Drone compacto com câmera.", 1300, 3500, 2),
    ("Eletronicos", "Fone Over-Ear", "Headphone over-ear com ANC.", 300, 1100, 4),
    ("Eletronicos", "Monitor Portátil", "Monitor USB-C 15 polegadas.", 800, 1800, 3),
    ("Eletronicos", "Projetor Mini", "Projetor compacto para ambientes.", 650, 1600, 2),
    # Informatica (15)
    ("Informatica", "Notebook 15\"", "Notebook 15 polegadas para trabalho.", 2500, 5500, 9),
    ("Informatica", "Ultrabook 13\"", "Ultrabook leve e potente.", 3800, 7500, 6),
    ("Informatica", "Mini PC", "Mini PC para escritório.", 1600, 3200, 5),
    ("Informatica", "Servidor Rack", "Servidor 1U para pequenas empresas.", 8500, 22000, 2),
    ("Informatica", "Roteador Wi-Fi 6", "Roteador dual band Wi-Fi 6.", 380, 980, 6),
    ("Informatica", "Switch 24 portas", "Switch gerenciável 24 portas.", 900, 2400, 4),
    ("Informatica", "NAS 2 Bay", "Storage NAS com 2 baias.", 1600, 4200, 3),
    ("Informatica", "Impressora Laser", "Impressora laser monocromática.", 900, 2200, 4),
    ("Informatica", "Scanner A4", "Scanner de documentos A4.", 600, 1500, 3),
    ("Informatica", "PC Gamer", "PC gamer intermediário.", 4200, 9000, 5),
    ("Informatica", "Workstation", "Estação de trabalho para criação.", 9500, 22000, 2),
    ("Informatica", "Nobreak 1500VA", "Nobreak para proteção elétrica.", 850, 1800, 3),
    ("Informatica", "Projetor Corporativo", "Projetor para salas de reunião.", 2200, 5200, 2),
    ("Informatica", "Impressora Térmica", "Impressora térmica para etiquetas.", 600, 1600, 2),
    ("Informatica", "Firewall Appliance", "Appliance de segurança de rede.", 1800, 5200, 2),
    # Perifericos (15)
    ("Perifericos", "Mouse Gamer", "Mouse gamer com DPI ajustável.", 120, 380, 9),
    ("Perifericos", "Teclado Mecânico", "Teclado mecânico switch azul.", 220, 680, 8),
    ("Perifericos", "Teclado Sem Fio", "Teclado sem fio slim.", 160, 420, 5),
    ("Perifericos", "Mouse Sem Fio", "Mouse sem fio ergonômico.", 120, 360, 6),
    ("Perifericos", "Monitor 24\"", "Monitor 24 polegadas IPS.", 650, 1300, 7),
    ("Perifericos", "Monitor 27\"", "Monitor 27 polegadas QHD.", 1200, 2400, 5),
    ("Perifericos", "Headset Gamer", "Headset com microfone removível.", 220, 780, 6),
    ("Perifericos", "Webcam HD", "Webcam 1080p com autofocus.", 180, 520, 5),
    ("Perifericos", "Microfone USB", "Microfone condensador USB.", 220, 750, 4),
    ("Perifericos", "Caixa de Som", "Kit 2.1 para escritório.", 180, 600, 4),
    ("Perifericos", "Controle Bluetooth", "Controle bluetooth para PC.", 180, 520, 3),
    ("Perifericos", "Mesa Digitalizadora", "Mesa digitalizadora para design.", 420, 1200, 3),
    ("Perifericos", "Cadeira Gamer", "Cadeira ergonômica gamer.", 800, 2200, 2),
    ("Perifericos", "Suporte Monitor", "Suporte articulado para monitor.", 220, 520, 3),
    ("Perifericos", "Dock USB-C", "Dock USB-C com várias portas.", 420, 1100, 4),
]


def _build_catalog_rows():
    rows = []
    for categoria, produto, descricao, preco_min, preco_max, peso in CATALOGO:
        preco_base = round((preco_min + preco_max) / 2, 2)
        rows.append(
            {
                "categoria": categoria,
                "categoria_descricao": CATEGORIAS[categoria],
                "produto": produto,
                "descricao": descricao,
                "preco_base": preco_base,
                "preco_min": preco_min,
                "preco_max": preco_max,
                "peso": peso,
            }
        )
    return rows


T = TypeVar("T")


def _build_cdf(weights: list[float]) -> list[float]:
    total = sum(weights)
    cdf = []
    acc = 0.0
    for w in weights:
        acc += w
        cdf.append(acc / total)
    return cdf


def _pick_from_cdf(options: Sequence[T], cdf: list[float]) -> T:
    r = random.random()
    idx = bisect(cdf, r)
    return options[idx]


def _jitter_weights(weights: list[float], min_factor: float, max_factor: float) -> list[float]:
    return [w * random.uniform(min_factor, max_factor) for w in weights]


def _build_date_cdf(start_date: datetime, end_date: datetime) -> tuple[list[datetime], list[float]]:
    month_base = [
        0.75,
        0.85,
        0.95,
        1.0,
        1.05,
        1.15,
        1.25,
        1.15,
        1.0,
        1.1,
        1.3,
        1.45,
    ]
    dow_base = [1.35, 1.2, 1.0, 1.0, 1.3, 0.6, 0.35]

    month_weights = _jitter_weights(month_base, 0.85, 1.25)
    dow_weights = _jitter_weights(dow_base, 0.8, 1.3)

    total_days = (end_date - start_date).days
    eventos = []
    if total_days > 60:
        for _ in range(random.randint(4, 7)):
            offset = random.randint(0, max(1, total_days - 30))
            duracao = random.randint(10, 28)
            inicio = start_date + timedelta(days=offset)
            fim = inicio + timedelta(days=duracao)
            boost = random.uniform(1.15, 1.6)
            eventos.append((inicio, fim, boost))
    dates: list[datetime] = []
    weights: list[float] = []
    for i in range(total_days + 1):
        day = start_date + timedelta(days=i)
        progress = i / total_days if total_days > 0 else 0
        trend = 0.8 + (0.5 * progress)
        weight = month_weights[day.month - 1] * dow_weights[day.weekday()] * trend
        if eventos:
            for inicio, fim, boost in eventos:
                if inicio <= day <= fim:
                    weight *= boost
        weight *= random.uniform(0.9, 1.1)
        dates.append(day)
        weights.append(weight)

    return dates, _build_cdf(weights)


def generate_dataset(rows: int, years: int, output_dir: Path, seed: int | None = None):
    if seed is not None:
        random.seed(seed)

    fake = Faker("pt_BR")
    catalog_rows = _build_catalog_rows()

    produtos = [r["produto"] for r in catalog_rows]
    pesos = [r["peso"] for r in catalog_rows]
    pesos = [max(1, int(p * random.uniform(0.7, 1.35))) for p in pesos]
    produtos_cdf = _build_cdf(pesos)
    produto_map = {r["produto"]: r for r in catalog_rows}
    acessorios = [r["produto"] for r in catalog_rows if r["categoria"] == "Acessorios"]

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    catalog_path = output_dir / f"catalogo_produtos_{timestamp}.csv"
    transacoes_path = output_dir / f"transacoes_{rows}_{timestamp}.csv"
    itens_path = output_dir / f"transacao_itens_{rows}_{timestamp}.csv"

    # Catálogo
    with catalog_path.open("w", newline="", encoding="utf-8") as catalog_file:
        writer = csv.writer(catalog_file)
        writer.writerow(
            [
                "categoria",
                "categoria_descricao",
                "produto",
                "descricao",
                "preco_base",
                "preco_min",
                "preco_max",
                "ativo",
            ]
        )
        for row in catalog_rows:
            writer.writerow(
                [
                    row["categoria"],
                    row["categoria_descricao"],
                    row["produto"],
                    row["descricao"],
                    f"{row['preco_base']:.2f}",
                    f"{row['preco_min']:.2f}",
                    f"{row['preco_max']:.2f}",
                    True,
                ]
            )

    # Clientes
    clientes = [fake.name() for _ in range(60000)]
    cliente_pesos = [max(1, int(random.paretovariate(1.25) * 10)) for _ in clientes]
    clientes_cdf = _build_cdf(cliente_pesos)

    status_options = ["pago", "pendente", "atrasado", "cancelado", "erro"]
    status_cdfs = {
        "Acessorios": _build_cdf([0.70, 0.18, 0.06, 0.04, 0.02]),
        "Componentes": _build_cdf([0.55, 0.20, 0.12, 0.08, 0.05]),
        "default": _build_cdf([0.60, 0.18, 0.10, 0.08, 0.04]),
    }

    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)
    datas_candidatas, datas_cdf = _build_date_cdf(start_date, end_date)

    with transacoes_path.open("w", newline="", encoding="utf-8") as trans_file, itens_path.open(
        "w", newline="", encoding="utf-8"
    ) as itens_file:
        trans_writer = csv.writer(trans_file)
        itens_writer = csv.writer(itens_file)

        trans_writer.writerow(
            [
                "id_transacao",
                "data_transacao",
                "cliente",
                "produto",
                "categoria",
                "valor",
                "status_pagamento",
                "data_pagamento",
            ]
        )
        itens_writer.writerow(
            [
                "id_transacao",
                "produto",
                "quantidade",
                "valor_unitario",
                "valor_total",
            ]
        )

        trans_chunk = []
        itens_chunk = []
        chunk_size = 20000

        for idx in range(1, rows + 1):
            id_transacao = f"TRX-PORT-{idx:09d}"
            produto_principal = _pick_from_cdf(produtos, produtos_cdf)
            produto_info = produto_map[produto_principal]
            categoria = produto_info["categoria"]
            cliente = _pick_from_cdf(clientes, clientes_cdf)
            data_transacao = _pick_from_cdf(datas_candidatas, datas_cdf)

            status_cdf = status_cdfs.get(categoria, status_cdfs["default"])
            status = _pick_from_cdf(status_options, status_cdf)

            data_pagamento = ""
            if status == "pago":
                pagamento = data_transacao + timedelta(days=random.randint(0, 15))
                if pagamento > end_date:
                    pagamento = end_date
                data_pagamento = pagamento.strftime("%d/%m/%Y")
            elif status == "atrasado":
                if random.random() < 0.35:
                    pagamento = data_transacao + timedelta(days=random.randint(20, 60))
                    if pagamento <= end_date:
                        data_pagamento = pagamento.strftime("%d/%m/%Y")

            r = random.random()
            if r < 0.55:
                itens_count = 1
            elif r < 0.85:
                itens_count = 2
            elif r < 0.97:
                itens_count = 3
            else:
                itens_count = 4

            itens = [produto_principal]
            while len(itens) < itens_count:
                if categoria != "Acessorios" and random.random() < 0.6:
                    itens.append(random.choice(acessorios))
                else:
                    itens.append(_pick_from_cdf(produtos, produtos_cdf))

            valor_total_transacao = 0.0
            for produto in itens:
                info = produto_map[produto]
                quantidade = 1 if random.random() < 0.8 else random.randint(2, 3)
                valor_unitario = random.uniform(info["preco_min"], info["preco_max"])
                valor_total_item = round(valor_unitario * quantidade, 2)
                valor_total_transacao += valor_total_item

                itens_chunk.append(
                    [
                        id_transacao,
                        produto,
                        quantidade,
                        f"{valor_unitario:.2f}",
                        f"{valor_total_item:.2f}",
                    ]
                )

            trans_chunk.append(
                [
                    id_transacao,
                    data_transacao.strftime("%d/%m/%Y"),
                    cliente,
                    produto_principal,
                    categoria,
                    f"{valor_total_transacao:.2f}",
                    status,
                    data_pagamento,
                ]
            )

            if idx % chunk_size == 0:
                trans_writer.writerows(trans_chunk)
                itens_writer.writerows(itens_chunk)
                trans_chunk.clear()
                itens_chunk.clear()
            if idx % 200000 == 0:
                print(f"Gerados {idx} registros...")

        if trans_chunk:
            trans_writer.writerows(trans_chunk)
        if itens_chunk:
            itens_writer.writerows(itens_chunk)

    print(f"Catálogo: {catalog_path}")
    print(f"Transacoes: {transacoes_path}")
    print(f"Itens: {itens_path}")


def main():
    parser = argparse.ArgumentParser(description="Gerar dados realistas para portfólio ETL.")
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    settings = get_settings()
    generate_dataset(args.rows, args.years, settings.raw_data_path, seed=args.seed)


if __name__ == "__main__":
    main()
