import os
import pandas as pd
import warnings

# Suprime ParserWarnings causados por diferença de colunas
warnings.simplefilter(action='ignore', category=pd.errors.ParserWarning)

def transform_cnpj(caminho_pasta, caminho_saida):
    """
    Transforma os arquivos de estabelecimentos do CNPJ em dois conjuntos de dados:
    1. estabelecimentos.csv -> Todas as 30 colunas do layout oficial + CNPJ_COMPLETO
    2. cnae_estabelecimentos.csv -> CNPJ completo + todos os CNAEs (primário e secundários)
    """

    estabelecimentos = []
    cnaes_estabelecimentos = []

    print(f"\n📁 Verificando arquivos em: {caminho_pasta}")
    for arquivo in os.listdir(caminho_pasta):
        if arquivo in [".DS_Store", "estabelecimentos.csv", "cnae_estabelecimentos.csv"]:
            continue

        caminho_arquivo = os.path.join(caminho_pasta, arquivo)
        print(f"\n🔍 Tentando ler: {arquivo}")

        try:
            df = pd.read_csv(
                caminho_arquivo,
                encoding="windows-1251",
                delimiter=";",
                names=list(range(30)),  # 30 colunas do layout oficial
                dtype=str
            )
        except Exception as e:
            print(f"⚠️ Erro ao ler {arquivo}: {e}")
            continue

        # Filtra estabelecimentos ativos ("02")
        if df.shape[1] != 30:
            print(f"⚠️ Arquivo {arquivo} com número incorreto de colunas. Pulando...")
            continue

        df = df[df[5] == "02"]  # Situação Cadastral = 02 (ativo)

        if df.empty:
            print(f"ℹ️ Nenhum estabelecimento ativo no arquivo {arquivo}.")
            continue

        # CNPJ COMPLETO
        df['CNPJ_COMPLETO'] = df[0] + df[1] + df[2]

        df = df.rename(columns={
            0: 'CNPJ_BASICO',
            1: 'CNPJ_ORDEM',
            2: 'CNPJ_DV',
            3: 'IDENT_MATRIZ_FILIAL',
            4: 'NOME_FANTASIA',
            5: 'SITUACAO_CADASTRAL',
            6: 'DATA_SITUACAO_CADASTRAL',
            7: 'MOTIVO_SITUACAO_CADASTRAL',
            8: 'NOME_CIDADE_EXTERIOR',
            9: 'PAIS',
            10: 'DATA_INICIO_ATIVIDADE',
            11: 'CNAE_PRIMARIO',
            12: 'CNAES_SECUNDARIOS',
            13: 'TIPO_LOGRADOURO',
            14: 'LOGRADOURO',
            15: 'NUMERO',
            16: 'COMPLEMENTO',
            17: 'BAIRRO',
            18: 'CEP',
            19: 'UF',
            20: 'MUNICIPIO',
            21: 'DDD_1',
            22: 'TELEFONE_1',
            23: 'DDD_2',
            24: 'TELEFONE_2',
            25: 'DDD_FAX',
            26: 'FAX',
            27: 'EMAIL',
            28: 'SITUACAO_ESPECIAL',
            29: 'DATA_SITUACAO_ESPECIAL'
        })

        # Adiciona CNPJ_COMPLETO à frente
        cols_ordenadas = ['CNPJ_COMPLETO'] + [col for col in df.columns if col != 'CNPJ_COMPLETO']
        df = df[cols_ordenadas]
        estabelecimentos.append(df)

        # Processa CNAEs
        cnae_df = df[['CNPJ_COMPLETO', 'CNAE_PRIMARIO', 'CNAES_SECUNDARIOS']].copy()
        cnae_df['CNAES_SECUNDARIOS'] = cnae_df['CNAES_SECUNDARIOS'].str.split(',')

        primario_df = cnae_df[['CNPJ_COMPLETO', 'CNAE_PRIMARIO']].rename(columns={'CNAE_PRIMARIO': 'CNAE'})
        secundario_df = cnae_df[['CNPJ_COMPLETO', 'CNAES_SECUNDARIOS']].explode('CNAES_SECUNDARIOS').dropna().rename(columns={'CNAES_SECUNDARIOS': 'CNAE'})

        cnaes_final_df = pd.concat([primario_df, secundario_df], ignore_index=True)
        cnaes_estabelecimentos.append(cnaes_final_df)

    if not estabelecimentos:
        print("⚠️ Nenhum arquivo foi processado.")
        return

    estabelecimentos_consolidado = pd.concat(estabelecimentos, ignore_index=True)
    cnaes_estabelecimentos_consolidado = pd.concat(cnaes_estabelecimentos, ignore_index=True)

    os.makedirs(caminho_saida, exist_ok=True)
    estabelecimentos_consolidado.to_csv(os.path.join(caminho_saida, 'estabelecimentos.csv'), index=False)
    cnaes_estabelecimentos_consolidado.to_csv(os.path.join(caminho_saida, 'cnae_estabelecimentos.csv'), index=False)

    print("\n✅ Transformação concluída.")
    print(" - estabelecimentos.csv")
    print(" - cnae_estabelecimentos.csv")


