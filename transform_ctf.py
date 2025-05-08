import os
import pandas as pd

def transform_ctf(caminho_pasta, caminho_saida):
    """
    Transforma os arquivos CSV de pessoas jurídicas do CTF/APP IBAMA
    em um único arquivo com CNPJ e código de atividade (ctf).

    Parâmetros:
    - caminho_pasta: pasta onde estão os arquivos .csv baixados.
    - caminho_saida: pasta onde o arquivo final consolidado será salvo.
    """

    dfs = []  # Lista para armazenar todos os DataFrames válidos

    for arquivo in os.listdir(caminho_pasta):
        # Ignora arquivos não CSV e arquivos indesejados
        if not arquivo.endswith('.csv') or arquivo in [".DS_Store", "CTF_final.csv"]:
            continue

        caminho_arquivo = os.path.join(caminho_pasta, arquivo)
        print(f"🔍 Lendo arquivo CTF: {caminho_arquivo}")

        try:
            df = pd.read_csv(
                caminho_arquivo,
                encoding="utf-8",
                delimiter=";",
                index_col=False,
                dtype=str
            )
        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")
            continue

        # Verifica se o DataFrame está vazio
        if df.empty:
            print(f"⚠️ Arquivo vazio ignorado: {arquivo}")
            continue

        # Aplica os filtros: apenas registros ativos e sem data de término
        df = df[df['Data de término da atividade'].isna()]
        df = df[df['Situação cadastral'] == "Ativa"]

        # Verifica novamente após os filtros se o DataFrame ainda tem conteúdo
        if df.empty:
            print(f"⚠️ Nenhum dado válido após filtros no arquivo: {arquivo}")
            continue

        # Criação da coluna "ctf" unindo código da categoria e da atividade
        df["ctf"] = df['Código da categoria'].map(str) + '-' + df['Código da atividade'].map(str)

        # Seleciona apenas colunas desejadas
        df = df[['CNPJ', 'ctf']].rename(columns={'CNPJ': 'cnpj'})
        dfs.append(df)

    # Valida se houve dados válidos antes de tentar concatenar
    if not dfs:
        print("⚠️ Nenhum dado consolidado. Nenhum arquivo válido encontrado.")
        return

    # Consolida os DataFrames e salva como CSV final
    df_final = pd.concat(dfs, ignore_index=True)
    os.makedirs(caminho_saida, exist_ok=True)
    caminho_final = os.path.join(caminho_saida, 'ctf_empresas.csv')
    df_final.to_csv(caminho_final, index=False)
    print(f"✅ Arquivo consolidado salvo em: {caminho_final}")
