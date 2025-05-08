import os
import pandas as pd
import warnings

# Suprime warnings de leitura
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)

def transform_cnpj_empresas(caminho_pasta, caminho_saida):
    """
    Transforma os arquivos do tipo EMPRESA da Receita Federal em:
    - dados_empresa.csv: CNPJ básico, razão social, natureza jurídica, capital social e porte
    """

    empresas = []

    print(f"\n📁 Verificando arquivos EMPRESA em: {caminho_pasta}")
    arquivos = sorted([f for f in os.listdir(caminho_pasta) if not f.endswith('.csv') and not f.startswith('.')])

    for i, arquivo in enumerate(arquivos, start=1):
        caminho_arquivo = os.path.join(caminho_pasta, arquivo)
        print(f"\n🔄 ({i}/{len(arquivos)}) Lendo: {arquivo}")

        try:
            df = pd.read_csv(
                caminho_arquivo,
                encoding="windows-1251",
                delimiter=";",
                names=list(range(0,10)),  # layout do arquivo EMPRESA
                index_col=False,
                dtype=str
            )
            print(f"   ➡️ {len(df)} registros carregados.")
        except Exception as e:
            print(f"⚠️ Erro ao ler {arquivo}: {e}")
            continue

        if df.shape[1] < 7:
            print(f"⚠️ Arquivo {arquivo} possui colunas insuficientes. Pulando...")
            continue

        # Renomeia colunas conforme layout Receita
        df = df.rename(columns={
            0: 'cnpj_basico',
            1: 'razao_social',
            2: 'natureza_juridica',
            5: 'capital_social',
            6: 'porte'
        })

        df = df[['cnpj_basico', 'razao_social', 'natureza_juridica', 'capital_social', 'porte']]
        empresas.append(df)

    if not empresas:
        print("⚠️ Nenhum dado de EMPRESA processado.")
        return

    os.makedirs(caminho_saida, exist_ok=True)
    df_final = pd.concat(empresas, ignore_index=True)
    df_final.to_csv(os.path.join(caminho_saida, 'dados_empresa.csv'), index=False)

    print(f"\n📋 Total de registros consolidados: {len(df_final)} registros.")
    print("\n✅ Transformação de EMPRESAS concluída.")
    print(" - Arquivo salvo: dados_empresa.csv")
