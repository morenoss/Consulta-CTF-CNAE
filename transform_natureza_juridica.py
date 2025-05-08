import os
import pandas as pd

def transform_naturezas_juridicas(caminho_arquivo_csv, caminho_saida):
    '''
    Transforma o arquivo de naturezas jurídicas da Receita Federal em um CSV legível.
    Entrada:
        - caminho_arquivo_csv: caminho completo do .csv baixado do site da Receita
        - caminho_saida: pasta onde o CSV final será salvo
    '''
    print(f"🔄 Lendo naturezas jurídicas: {caminho_arquivo_csv}")

    try:
        df = pd.read_csv(
            caminho_arquivo_csv,
            sep=';',
            header=None,
            names=['codigo', 'descricao'],
            dtype=str,
            encoding='latin1'
        )
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo: {e}")
        return

    # Valida se o DataFrame tem conteúdo
    if df.empty or df.shape[1] < 2:
        print("⚠️ Arquivo lido está vazio ou incompleto. Transformação cancelada.")
        return

    os.makedirs(caminho_saida, exist_ok=True)
    caminho_final = os.path.join(caminho_saida, 'naturezas_juridicas.csv')
    df.to_csv(caminho_final, index=False)
    print(f"✅ Transformação concluída. Arquivo salvo em: {caminho_final}")

