import os
import time
import threading
from get_files_online import get_files_online, get_latest_cnpj_urls
from transform_cnpj_estabelecimentos import transform_cnpj as transform_estab
from transform_cnpj_empresas import transform_cnpj_empresas
from transform_ctf import transform_ctf
from transform_natureza_juridica import transform_naturezas_juridicas
from transform_cnae import transform_cnae
from export_to_parquet import exportar_para_parquet

# Diretório do projeto
dir_atual = os.path.dirname(os.path.abspath(__file__))

# Diretórios locais para salvar os dados
estab_dir = os.path.join(dir_atual, 'Dados CNPJ Estab')
empresas_dir = os.path.join(dir_atual, 'Dados CNPJ Empresas')
ctf_dir = os.path.join(dir_atual, 'Dados CTF IBAMA')
output_dir = os.path.join(dir_atual, 'Entrada do Painel')
parquet_dir = os.path.join(dir_atual, 'Dados Painel Parquet')

# URL base dos dados abertos do CNPJ
base_cnpj_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

# Função auxiliar para aguardar o ENTER ou timeout
def esperar_enter(timeout=30):
    """
    Aguarda o pressionamento da tecla ENTER ou inicia automaticamente após o timeout (em segundos).
    """
    print("\n⏳ Pressione ENTER para iniciar imediatamente...")
    print(f"⚡ Caso não pressione, o pipeline continuará automaticamente em {timeout} segundos.")

    # Variável de controle
    enter_detectado = []

    def aguardar():
        input()
        enter_detectado.append(True)

    thread = threading.Thread(target=aguardar)
    thread.daemon = True
    thread.start()

    for _ in range(timeout):
        if enter_detectado:
            print("\n▶️ Iniciando imediatamente!\n")
            return
        time.sleep(1)

    print("\n⌛ Tempo esgotado! Continuando o pipeline...\n")

# ==============================================
# Apresentação inicial do pipeline
# ==============================================

print("=" * 60)
print("🔄 PIPELINE DE DADOS CNPJ E CTF IBAMA")
print("=" * 60)

print("\n📌 Este pipeline tem como objetivo atualizar os dados utilizados no modelo semântico")
print("do relatório de Power BI denominado 'Consulta CTF por Descrição ou Código CNAE'.")
print("Ele realiza o download, extração e transformação dos principais conjuntos de dados")
print("necessários para garantir a integridade e atualidade da base utilizada no relatório.")

print("\n📦 Arquivos finais gerados por este pipeline (em formato CSV):")

print("\n - estabelecimentos.csv:")
print("   📄 Contém: CNPJ Básico, CNPJ Completo, Nome Fantasia, Identificador Matriz/Filial,")
print("             Data de Início de Atividade, Tipo de Logradouro, Logradouro, Número,")
print("             Complemento, Bairro, CEP, UF, Município, Telefones, Fax, E-mail.")

print("\n - cnae_estabelecimentos.csv:")
print("   📄 Contém: CNPJ Completo e Código CNAE (Primário e Secundários separados).")

print("\n - dados_empresa.csv:")
print("   📄 Contém: CNPJ Básico, Razão Social, Natureza Jurídica, Capital Social, Porte.")

print("\n - ctf_empresas.csv:")
print("   📄 Contém: CNPJ Completo e Código da Atividade (CTF/IBAMA).")

print("\n - naturezas_juridicas.csv:")
print("   📄 Contém: Código, Descrição, Natureza, Qualificação, Data de Início e Data de Fim.")

print("\n - cnaes.csv:")
print("   📄 Contém: Código CNAE e Descrição oficial, conforme tabela da Receita Federal.")

print("\n🧊 Todos esses arquivos também são automaticamente convertidos para o formato Parquet,")
print("   que é mais eficiente para o Power BI por ser compacto, colunar e de leitura mais rápida.")
print(f"   📂 Pasta: {os.path.join(dir_atual, 'Dados Painel Parquet')}")
print("   📄 Caminho salvo em: caminho_dados_parquet.txt")

print("\nEste pipeline agendado realizará as seguintes etapas:")
print("\n1️⃣  Download e transformação dos dados de estabelecimentos do CNPJ:")
print("    - Scripts: get_files_online.py, transform_cnpj_estabelecimentos.py")
print("    - Funções: get_latest_cnpj_urls(), get_files_online(), transform_cnpj()\n")

print("2️⃣  Download e transformação dos dados das empresas (matriz) do CNPJ:")
print("    - Scripts: get_files_online.py, transform_cnpj_empresas.py")
print("    - Funções: get_latest_cnpj_urls(), get_files_online(), transform_cnpj_empresas()\n")

print("3️⃣  Download e transformação dos dados do CTF (IBAMA):")
print("    - Scripts: get_files_online.py, transform_ctf.py")
print("    - Funções: get_files_online(), transform_ctf()\n")

print("4️⃣  Download e transformação dos dados de Natureza Jurídica do CNPJ:")
print("    - Scripts: get_files_online.py, transform_natureza_juridica.py")
print("    - Funções: get_latest_cnpj_urls(), get_files_online(), transform_naturezas_juridicas()\n")

print("🗃️ Os resultados finais serão armazenados em:")
print(f"    📂 CSV:    {output_dir}")
print(f"    📂 Parquet: {parquet_dir}")

# Pausa para ENTER ou timeout
esperar_enter(timeout=30)

# --------------------------------------------------------------------------
# PARTE 1 - DADOS DE ESTABELECIMENTOS
# --------------------------------------------------------------------------
print("\n===== PARTE 1: DADOS DE ESTABELECIMENTOS (CNPJ) =====")
estab_urls = get_latest_cnpj_urls(base_cnpj_url, tipo_arquivo='Estabelecimentos')
get_files_online(estab_urls, estab_dir)
transform_estab(estab_dir, output_dir)

# --------------------------------------------------------------------------
# PARTE 2 - DADOS DE EMPRESAS (MATRIZ)
# --------------------------------------------------------------------------
print("\n===== PARTE 2: DADOS DE EMPRESAS (RAZÃO SOCIAL) =====")
print("🔎 Iniciando download dos arquivos de Empresas (matriz)...")
empresas_urls = get_latest_cnpj_urls(base_cnpj_url, tipo_arquivo='Empresas')
get_files_online(empresas_urls, empresas_dir)

print("🔄 Iniciando transformação dos arquivos de Empresas (matriz)...")
transform_cnpj_empresas(empresas_dir, output_dir)

# --------------------------------------------------------------------------
# PARTE 3 - DADOS CTF IBAMA
# --------------------------------------------------------------------------
print("\n===== PARTE 3: DADOS DO CTF (IBAMA) =====")
url_base_ctf = "http://dadosabertos.ibama.gov.br/dados/CTF/APP/"
estados = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
    "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC",
    "SP","SE","TO"
]
ctf_urls = [f"{url_base_ctf}{uf}/pessoasJuridicas.csv" for uf in estados]
get_files_online(ctf_urls, ctf_dir)
transform_ctf(ctf_dir, output_dir)

# --------------------------------------------------------------------------
# PARTE 4 - DADOS DE NATUREZA JURÍDICA
# --------------------------------------------------------------------------
print("\n===== PARTE 4: NATUREZAS JURÍDICAS (CNPJ) =====")
naturezas_urls = get_latest_cnpj_urls(base_cnpj_url, tipo_arquivo='Naturezas')
get_files_online(naturezas_urls, estab_dir)

# Localizar dinamicamente o arquivo NATJUCSV extraído
arquivo_natureza_csv = None
for arquivo in os.listdir(estab_dir):
    if arquivo.upper().endswith('NATJUCSV'):
        arquivo_natureza_csv = os.path.join(estab_dir, arquivo)
        break

if arquivo_natureza_csv:
    print(f"🔄 Lendo naturezas jurídicas: {arquivo_natureza_csv}")
    transform_naturezas_juridicas(arquivo_natureza_csv, output_dir)
else:
    print("⚠️ Nenhum arquivo CSV de natureza jurídica encontrado após extração!")
    
# --------------------------------------------------------------------------
# PARTE 5 - CNAEs
# --------------------------------------------------------------------------
print("\n===== PARTE 5: CNAEs (Códigos e Descrições) =====")
cnae_urls = get_latest_cnpj_urls(base_cnpj_url, tipo_arquivo='Cnaes')
get_files_online(cnae_urls, estab_dir)

# Localiza arquivo com "CNAECSV"
# Usa diretamente o arquivo renomeado pelo get_files_online
arquivo_cnae_csv = os.path.join(estab_dir, "cnaes_original.csv")
if os.path.exists(arquivo_cnae_csv):
    print(f"🔄 Lendo CNAEs: {arquivo_cnae_csv}")
    transform_cnae(arquivo_cnae_csv, output_dir)
else:
    print("⚠️ Arquivo 'cnaes_original.csv' não encontrado após extração!")
    
# --------------------------------------------------------------------------
#PARTE 6 - EXPORTAÇÃO PARA FORMATO PARQUET
# --------------------------------------------------------------------------
print("\n===== EXPORTANDO ARQUIVOS PARA FORMATO PARQUET =====")

from export_to_parquet import exportar_para_parquet

# Pasta de destino para arquivos Parquet
pasta_parquet = os.path.join(dir_atual, 'Dados Painel Parquet')

# Executa a exportação
exportar_para_parquet(origem=output_dir, destino=pasta_parquet)

print("\n✅ Exportação para Parquet concluída com sucesso!")

# --------------------------------------------------------------------------
# FINALIZAÇÃO
# --------------------------------------------------------------------------
print("\n===== PIPELINE COMPLETO! =====\n")
print("📁 Arquivos gerados:")

print("\n🔹 Estabelecimentos (estabelecimentos.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'estabelecimentos.csv'))}")
print("   - Colunas: CNPJ Básico, CNPJ Completo, Nome Fantasia, Identificador Matriz/Filial, Data de Início de Atividade,")
print("              Tipo de Logradouro, Logradouro, Número, Complemento, Bairro, CEP, UF, Município, Telefones, Fax, E-mail.\n")

print("🔹 CNAEs dos Estabelecimentos (cnae_estabelecimentos.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'cnae_estabelecimentos.csv'))}")
print("   - Colunas: CNPJ Completo, CNAE (Primário e Secundários separados).\n")

print("🔹 Empresas Matriz (dados_empresa.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'dados_empresa.csv'))}")
print("   - Colunas: CNPJ Básico, Razão Social, Natureza Jurídica, Capital Social, Porte da Empresa.\n")

print("🔹 Cadastro Técnico Federal - CTF (ctf_empresas.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'ctf_empresas.csv'))}")
print("   - Colunas: CNPJ Completo, Código da Atividade (CTF/IBAMA).\n")

print("🔹 Naturezas Jurídicas (naturezas_juridicas.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'naturezas_juridicas.csv'))}")
print("   - Colunas: Código, Descrição, Natureza, Qualificação, Data de Início, Data de Fim.\n")

print("🔹 CNAEs (cnae.csv)")
print(f"   - Local: {os.path.abspath(os.path.join(output_dir, 'cnaes.csv'))}")
print("   - Colunas: cnae, desc_cnae.\n")

input("\n ▶️ Pressione ENTER para fechar...")
