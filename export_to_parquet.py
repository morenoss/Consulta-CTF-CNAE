import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

def exportar_para_parquet(
    origem="Entrada do Painel",
    destino="Dados Painel Parquet",
    ignorar=("ctfs.csv",),
    chunk_size=500_000
):
    """
    Converte todos os arquivos CSV da pasta de origem para Parquet, salvando na pasta de destino.

    - Para 'estabelecimentos.csv', faz leitura em chunks e escreve vários row-groups
      em um único arquivo Parquet, evitando estouro de memória.
    - Para os demais, converte diretamente com pandas -> Parquet.
    - Usa tqdm para mostrar progresso.
    """

    os.makedirs(destino, exist_ok=True)
    arquivos_csv = [
        f for f in os.listdir(origem)
        if f.endswith(".csv") and f not in ignorar
    ]

    if not arquivos_csv:
        print("⚠️ Nenhum arquivo CSV encontrado na pasta de origem.")
        return

    print("=" * 60)
    print("💾 EXPORTAÇÃO PARA FORMATO PARQUET")
    print("=" * 60)

    progresso = tqdm(arquivos_csv, desc="Convertendo arquivos", unit="arquivo", ncols=100)

    for arquivo in progresso:
        caminho_csv     = os.path.join(origem, arquivo)
        caminho_parquet = os.path.join(destino, arquivo.replace(".csv", ".parquet"))

        # Se for o estabelecimentos.csv, usar chunking
        if arquivo.lower() == "estabelecimentos.csv":
            primeiro = True
            writer = None
            leitor = pd.read_csv(
                caminho_csv,
                dtype=str,
                chunksize=chunk_size,
                iterator=True,
                encoding="utf-8"
            )
            for chunk in leitor:
                tabela = pa.Table.from_pandas(chunk, preserve_index=False)
                if primeiro:
                    writer = pq.ParquetWriter(
                        caminho_parquet,
                        schema=tabela.schema,
                        compression="snappy"
                    )
                    primeiro = False
                writer.write_table(tabela)
            if writer:
                writer.close()
                progresso.write(f"✅ {arquivo} convertido com chunking.")
            else:
                progresso.write(f"❌ Falha ao abrir writer para {arquivo}.")
        else:
            # conversão padrão para arquivos menores
            try:
                df = pd.read_csv(caminho_csv, dtype=str, encoding="utf-8")
                df.to_parquet(
                    caminho_parquet,
                    index=False,
                    engine="pyarrow",
                    compression="snappy"
                )
                progresso.write(f"✅ {arquivo} convertido com sucesso.")
            except Exception as e:
                progresso.write(f"❌ Erro ao processar {arquivo}: {e}")

    print(f"\n📁 Arquivos Parquet salvos em: {os.path.abspath(destino)}")
    print("=" * 60)


if __name__ == "__main__":
    exportar_para_parquet()

