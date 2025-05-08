import os
import pandas as pd
from tqdm import tqdm

def exportar_para_parquet(origem="Entrada do Painel", destino="Dados Painel Parquet", ignorar=["ctfs.csv"]):
    """
    Converte todos os arquivos CSV da pasta de origem para Parquet, salvando na pasta de destino.

    - Melhora performance no Power BI ao usar arquivos colunares.
    - Usa tqdm para mostrar o progresso de conversão.
    - Garante que a pasta de destino exista.
    """

    os.makedirs(destino, exist_ok=True)
    arquivos_csv = [f for f in os.listdir(origem) if f.endswith(".csv") and f not in ignorar]

    if not arquivos_csv:
        print("⚠️ Nenhum arquivo CSV encontrado na pasta de origem.")
        return

    print("=" * 60)
    print("💾 EXPORTAÇÃO PARA FORMATO PARQUET")

    caminho_txt = os.path.join(destino, "..", "caminho_dados_parquet.txt")
    try:
        with open(caminho_txt, "w", encoding="utf-8") as f:
            f.write(os.path.abspath(destino))
        print(f"📄 Caminho salvo em: {os.path.abspath(caminho_txt)}")
    except Exception as e:
        print(f"❌ Erro ao salvar caminho de destino: {e}")

    print("=" * 60)

    progresso = tqdm(arquivos_csv, desc="Convertendo arquivos", unit="arquivo", ncols=100)

    for arquivo in progresso:
        caminho_csv = os.path.join(origem, arquivo)
        caminho_parquet = os.path.join(destino, arquivo.replace(".csv", ".parquet"))

        try:
            df = pd.read_csv(caminho_csv, dtype=str)
            df.to_parquet(caminho_parquet, index=False, engine="pyarrow")
            if os.path.exists(caminho_parquet):
                progresso.write(f"✅ {arquivo} convertido com sucesso.")
            else:
                progresso.write(f"⚠️ {arquivo} processado, mas o Parquet não foi salvo.")
        except Exception as e:
            progresso.write(f"❌ Erro ao processar {arquivo}: {e}")

    print(f"\n📁 Arquivos Parquet salvos em: {os.path.abspath(destino)}")
    print("=" * 60)

if __name__ == "__main__":
    exportar_para_parquet()

