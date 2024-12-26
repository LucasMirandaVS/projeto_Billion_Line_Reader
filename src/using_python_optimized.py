from csv import reader
from tqdm import tqdm  # Barra de progresso
import time
from collections import defaultdict

NUMERO_DE_LINHAS = 1_000_000_000

def processar_temperaturas(path_do_csv):
    minimas = {}
    maximas = {}
    somas = {}
    medicoes = {}

    with open(path_do_csv, 'r', encoding='utf-8', buffering=1024*1024) as file:
        _reader = reader(file, delimiter=';')
        next(_reader, None)  # Ignorar o cabeçalho, se existir

        for row in tqdm(_reader, total=NUMERO_DE_LINHAS, desc="Processando"):
            try:
                nome_da_station = row[0]
                temperatura = float(row[1])

                if nome_da_station not in minimas:
                    minimas[nome_da_station] = temperatura
                    maximas[nome_da_station] = temperatura
                    somas[nome_da_station] = temperatura
                    medicoes[nome_da_station] = 1
                else:
                    minimas[nome_da_station] = min(minimas[nome_da_station], temperatura)
                    maximas[nome_da_station] = max(maximas[nome_da_station], temperatura)
                    somas[nome_da_station] += temperatura
                    medicoes[nome_da_station] += 1

            except (ValueError, IndexError):
                continue  # Ignorar linhas mal formatadas

    print("Dados carregados. Calculando estatísticas...")

    results = {
        station: (
            min_temp,
            somas[station] / medicoes[station],
            max_temp
        )
        for station, min_temp, max_temp in zip(minimas.keys(), minimas.values(), maximas.values())
    }

    print("Estatística calculada. Ordenando...")
    sorted_results = dict(sorted(results.items()))

    formatted_results = {
        station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
        for station, (min_temp, mean_temp, max_temp) in sorted_results.items()
    }

    return formatted_results


if __name__ == "__main__":
    path_do_csv = "data/measurements.txt"

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()

    resultados = processar_temperaturas(path_do_csv)

    end_time = time.time()

    for station, metrics in resultados.items():
        print(station, metrics, sep=': ')

    print(f"\nProcessamento concluído em {end_time - start_time:.2f} segundos.")
