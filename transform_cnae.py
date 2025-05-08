import os
import pandas as pd

def transform_cnae(arquivo_csv, caminho_saida):
    """
    Transforma o arquivo CNAE da Receita Federal, extraído do Cnaes.zip,
    renomeando as colunas para 'cnae' e 'desc_cnae' e salvando o CSV final.
    """
    try:
        df = pd.read_csv(
            arquivo_csv,
            encoding="windows-1252",
            delimiter=";",
            names=["cnae", "desc_cnae"],
            header=None,
            dtype=str
        )
    except Exception as e:
        print(f"⚠️ Erro ao ler o arquivo CNAE: {e}")
        return

    os.makedirs(caminho_saida, exist_ok=True)
    caminho_saida_arquivo = os.path.join(caminho_saida, "cnaes.csv")
    df.to_csv(caminho_saida_arquivo, index=False)

    print(f"✅ Transformação concluída. Arquivo salvo em: {caminho_saida_arquivo}")
