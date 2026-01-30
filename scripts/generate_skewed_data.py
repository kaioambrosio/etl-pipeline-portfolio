import argparse
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


def _build_product_catalog():
    products = [
        ("Notebook", "Informatica", 14),
        ("Smartphone", "Eletronicos", 12),
        ("Placa de Video", "Componentes", 10),
        ("Processador", "Componentes", 9),
        ("RAM", "Componentes", 8),
        ("SSD", "Componentes", 7),
        ("Monitor", "Perifericos", 8),
        ("Teclado", "Perifericos", 7),
        ("Mouse", "Perifericos", 7),
        ("Headset", "Perifericos", 6),
        ("Webcam", "Perifericos", 4),
        ("Gabinete", "Componentes", 5),
        ("Fonte", "Componentes", 5),
        ("Tablet", "Eletronicos", 4),
        ("Cooler", "Componentes", 3),
        ("Carregador", "Acessorios", 6),
        ("Cabo HDMI", "Acessorios", 5),
        ("Mousepad", "Acessorios", 4),
        ("Hub USB", "Acessorios", 3),
    ]
    items = [{"produto": p, "categoria": c, "peso": w} for p, c, w in products]
    return items


def _build_client_pool(fake: Faker, size: int = 2000):
    clients = [fake.name() for _ in range(size)]
    weights = []
    for _ in clients:
        weights.append(max(1, int(random.paretovariate(1.3) * 10)))
    return clients, weights


def _pick_date(start_date: datetime, end_date: datetime):
    years = list(range(start_date.year, end_date.year + 1))
    year_weights = [i + 1 for i in range(len(years))]
    year = random.choices(years, weights=year_weights, k=1)[0]

    if year == start_date.year and year == end_date.year:
        min_month = start_date.month
        max_month = end_date.month
    elif year == start_date.year:
        min_month = start_date.month
        max_month = 12
    elif year == end_date.year:
        min_month = 1
        max_month = end_date.month
    else:
        min_month = 1
        max_month = 12

    months = list(range(min_month, max_month + 1))
    month_weights = list(reversed(range(1, len(months) + 1)))
    month = random.choices(months, weights=month_weights, k=1)[0]

    if year == start_date.year and month == start_date.month:
        min_day = min(start_date.day, 28)
        max_day = 28
    elif year == end_date.year and month == end_date.month:
        min_day = 1
        max_day = min(end_date.day, 28)
    else:
        min_day = 1
        max_day = 28

    if min_day > max_day:
        min_day = 1

    day = random.randint(min_day, max_day)
    return datetime(year, month, day)


def _pick_status():
    statuses = ["pago", "pendente", "atrasado", "cancelado", "erro"]
    weights = [0.60, 0.18, 0.10, 0.08, 0.04]
    return random.choices(statuses, weights=weights, k=1)[0]


def _pick_value():
    r = random.random()
    if r < 0.60:
        return random.uniform(50, 2000)
    if r < 0.85:
        return random.uniform(2000, 10000)
    if r < 0.95:
        return random.uniform(10000, 50000)
    return random.uniform(50000, 300000)


def generate(
    rows: int,
    output_path: Path,
    seed: int | None = None,
    years: int = 5,
):
    if seed is not None:
        random.seed(seed)

    fake = Faker("pt_BR")

    catalog = _build_product_catalog()
    products = [item["produto"] for item in catalog]
    product_weights = [item["peso"] for item in catalog]
    product_to_category = {item["produto"]: item["categoria"] for item in catalog}

    clients, client_weights = _build_client_pool(fake)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 365)

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
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

        for idx in range(1, rows + 1):
            data_transacao = _pick_date(start_date, end_date)
            status = _pick_status()
            produto = random.choices(products, weights=product_weights, k=1)[0]
            categoria = product_to_category[produto]
            cliente = random.choices(clients, weights=client_weights, k=1)[0]
            valor = _pick_value()

            data_pagamento = ""
            if status == "pago":
                data_pagamento = (data_transacao + timedelta(days=random.randint(0, 15))).strftime(
                    "%d/%m/%Y"
                )
            elif status == "atrasado":
                if random.random() < 0.35:
                    data_pagamento = (
                        data_transacao + timedelta(days=random.randint(20, 60))
                    ).strftime("%d/%m/%Y")

            writer.writerow(
                [
                    f"TRX-SK-{idx:07d}",
                    data_transacao.strftime("%d/%m/%Y"),
                    cliente,
                    produto,
                    categoria,
                    f"{valor:.2f}",
                    status,
                    data_pagamento,
                ]
            )


def main():
    parser = argparse.ArgumentParser(description="Generate skewed load test data.")
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--years", type=int, default=5)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()

    settings = get_settings()
    output = (
        Path(args.output)
        if args.output
        else settings.raw_data_path
        / f"performance_test_skew_{args.rows}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    generate(args.rows, output, seed=args.seed, years=args.years)
    print(f"Generated: {output}")


if __name__ == "__main__":
    main()
