
import subprocess
import sys
import os
import time
import io

# Força UTF-8 no stdout se necessário (evita erro com emojis no agendador)
if not sys.stdout.encoding.lower().startswith("utf"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Função para instalar pacotes usando o requirements.txt
def instalar_requisitos():
    requirements_file = "requirements.txt"
    if not os.path.exists(requirements_file):
        with open(requirements_file, "w") as f:
            f.write("pandas\nrequests\ntqdm\nbeautifulsoup4\npyarrow\n")
        print("📝 requirements.txt criado automaticamente.")

    print("📦 Instalando/Atualizando pacotes do requirements.txt...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file, "--upgrade"])

# Função para criar diretórios se não existirem
def criar_pastas(pastas):
    for pasta in pastas:
        if not os.path.exists(pasta):
            os.makedirs(pasta)
            print(f"📁 Pasta criada: {pasta}")
        else:
            print(f"📁 Pasta já existe: {pasta}")

# Função para executar o pipeline
def executar_pipeline(script_path):
    print(f"\n🚀 Executando pipeline: {script_path}\n")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        print("✅ Pipeline executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao executar {script_path} - Código de retorno: {e.returncode}")
    except Exception as ex:
        print(f"❌ Erro inesperado: {ex}")

if __name__ == "__main__":
    print("=" * 60)
    print("🛠️  SETUP AUTOMÁTICO - Consulta-CTF-CNAE")
    print("=" * 60)

    # Instalar requisitos
    instalar_requisitos()

    # Criar pastas necessárias
    diretorios = [
    "Dados CNPJ Estab",
    "Dados CNPJ Empresas",
    "Dados CNAE",
    "Dados Natureza Jurídica",
    "Dados CTF IBAMA",
    "Entrada do Painel",
    "Dados Painel Parquet"
    ]
    criar_pastas(diretorios)

    time.sleep(2)

    # Executar o run.py
    caminho_run = os.path.join(os.getcwd(), "run.py")
    if os.path.exists(caminho_run):
        executar_pipeline(caminho_run)
    else:
        print(f"❌ run.py não encontrado em: {caminho_run}")

    print("\n📋 Pacotes instalados no ambiente:")
    subprocess.run([sys.executable, "-m", "pip", "list"])

    if sys.stdin.isatty():
        input("\n⏹️ Pressione ENTER para encerrar...")
