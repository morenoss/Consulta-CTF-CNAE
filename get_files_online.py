import os
import requests
from tqdm import tqdm
from zipfile import ZipFile
from bs4 import BeautifulSoup

def get_files_online(urls, caminho_destino):
    
    """
    Faz download e extração de arquivos (ZIP ou CSV) a partir de uma lista de URLs.

    - Suporta arquivos ZIP com extração automática
    - Mostra barra de progresso para cada arquivo
    - Salva todos os arquivos em 'caminho_destino'
    """

    os.makedirs(caminho_destino, exist_ok=True)

    for i, url in enumerate(urls):
        nome_arquivo = f"file_{i}_" + os.path.basename(url)
        caminho_arquivo = os.path.join(caminho_destino, nome_arquivo)

        print(f"\n🔄 Baixando: {url}")
        try:
            resposta = requests.get(url, stream=True, timeout=30, verify=False)
            resposta.raise_for_status()
        except Exception as e:
            print(f"❌ Erro ao baixar {url}: {e}")
            continue

        total = int(resposta.headers.get('content-length', 0))

        try:
            with open(caminho_arquivo, 'wb') as f, tqdm(
                desc=f"    🟢 {nome_arquivo}",
                total=total if total > 0 else None,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                dynamic_ncols=True,
                leave=True
            ) as barra:
                for dados in resposta.iter_content(chunk_size=1024 * 1024):  # 1 MB
                    if dados:
                        f.write(dados)
                        barra.update(len(dados))
            print(f"✅ Download concluído: {caminho_arquivo}")
        except Exception as e:
            print(f"❌ Erro ao salvar {nome_arquivo}: {e}")
            continue

        # Extração se for ZIP
        if nome_arquivo.endswith(".zip"):
            try:
                print(f"📦 Extraindo: {nome_arquivo}")
                with ZipFile(caminho_arquivo, 'r') as zip_ref:
                        zip_ref.extractall(caminho_destino)
                        for nome in zip_ref.namelist():
                            if "CNAE" in nome.upper() and nome.upper().endswith("CSV"):
                                caminho_origem = os.path.join(caminho_destino, nome)
                                caminho_destino_csv = os.path.join(caminho_destino, "cnaes_original.csv")
                                os.rename(caminho_origem, caminho_destino_csv)
                os.remove(caminho_arquivo)
                print(f"🗑️ Deletando ZIP: {nome_arquivo}")
            except Exception as e:
                print(f"❌ Erro ao extrair {nome_arquivo}: {e}")

    print(f"\n📁 Todos os arquivos foram processados em: {caminho_destino}")


def get_latest_cnpj_urls(url_base, tipo_arquivo='Estabelecimentos'):
   
    """
    Retorna os links dos arquivos mais recentes da Receita Federal
    para o tipo especificado (ex: Estabelecimentos, Empresas, Socios...).
    """

    try:
        resposta = requests.get(url_base, stream=True, timeout=30, verify=False)
        resposta.raise_for_status()
    except Exception as e:
        raise Exception(f"Erro ao acessar a URL base: {e}")

    soup = BeautifulSoup(resposta.text, 'html.parser')

    meses = sorted(set(link['href'].strip('/') for link in soup.find_all('a', href=True)
                       if link['href'].startswith('20')), reverse=True)

    if not meses:
        raise Exception("Nenhum diretório de mês encontrado no site da Receita.")

    url_mais_recente = url_base + meses[0] + "/"
    print(f"🗂️  Usando dados mais recentes de: {url_mais_recente}")

    try:
        resposta_mes = requests.get(url_mais_recente, timeout=30)
        resposta_mes.raise_for_status()
    except Exception as e:
        raise Exception(f"Erro ao acessar o diretório mais recente: {e}")

    soup_mes = BeautifulSoup(resposta_mes.text, 'html.parser')
    links_mes = soup_mes.find_all('a', href=True)

    return [url_mais_recente + link['href'] for link in links_mes if tipo_arquivo in link['href']]

def baixar_arquivo_simples(url, destino, nome_arquivo):
    
    """
    Baixa um único arquivo CSV (ex: naturezas jurídicas) para uma pasta destino com nome definido.

    Parâmetros:
    - url: link direto do arquivo CSV a ser baixado
    - destino: pasta onde o arquivo será salvo
    - nome_arquivo: nome com o qual o arquivo será salvo localmente
    """
    
    os.makedirs(destino, exist_ok=True)
    caminho_arquivo = os.path.join(destino, nome_arquivo)

    print(f"\n🔄 Baixando arquivo: {url}")
    try:
        resposta = requests.get(url, timeout=30, verify=False)  # SSL desativado por segurança pública do site
        resposta.raise_for_status()
    except Exception as e:
        print(f"❌ Erro ao baixar {url}: {e}")
        return None

    try:
        with open(caminho_arquivo, 'wb') as f:
            f.write(resposta.content)
        print(f"✅ Arquivo salvo em: {caminho_arquivo}")
        return caminho_arquivo
    except Exception as e:
        print(f"❌ Erro ao salvar {nome_arquivo}: {e}")
        return None

