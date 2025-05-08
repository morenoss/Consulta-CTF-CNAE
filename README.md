
# Consulta-CTF-CNAE

Análise e consolidação de dados públicos da Receita Federal e do IBAMA utilizando Python, com foco em aplicações práticas no planejamento de compras públicas sustentáveis.

---

## 🎯 Objetivo

Este repositório contém um conjunto de scripts em Python para:

- Baixar os dados abertos do Cadastro Nacional da Pessoa Jurídica (CNPJ) da Receita Federal
- Baixar os dados do Cadastro Técnico Federal (CTF/APP) do IBAMA
- Consolidar e transformar essas informações para posterior análise em ferramentas como Power BI

O projeto permite responder a perguntas como:

- _Quantas empresas de determinada atividade econômica estão registradas no CTF do IBAMA?_
- _Quais são os CNAEs mais associados a empresas com inscrição ativa no IBAMA?_
- _Qual o porte ou capital social médio dessas empresas?_

---

## 🧠 Justificativa

A **Nova Lei de Licitações (Lei n. 14.133/2021)** reforça o princípio do **desenvolvimento nacional sustentável**.  
O **Guia de Licitações Sustentáveis da AGU** recomenda, por exemplo, a exigência do **Certificado de Regularidade junto ao IBAMA**.

Este repositório fornece uma base consolidada que pode **auxiliar a Administração Pública no planejamento de contratações e aquisições sustentáveis**, com base em evidências reais e atualizadas.

---

## 📦 Fontes de Dados

### 🔹 Receita Federal
- Dados abertos do CNPJ: [Portal de Dados Abertos](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- Bases utilizadas:
  - **Estabelecimentos** – CNPJ completo, nome fantasia, endereço, contatos e CNAEs
  - **Empresas (Matriz)** – CNPJ básico, razão social, natureza jurídica, porte e capital social
  - **Naturezas Jurídicas** – Código e descrição da natureza jurídica

### 🔹 IBAMA
- Cadastro Técnico Federal de Atividades Potencialmente Poluidoras e Utilizadoras de Recursos Naturais (CTF/APP)
- Fonte: [Portal de Dados Abertos do IBAMA](https://dadosabertos.ibama.gov.br/dataset/pessoas-juridicas-inscritas-no-ctf-app)
- Considerações:
  - Apenas empresas com **situação ativa**
  - Apenas empresas **sem data de término** da atividade
- ⚠️ A inscrição no CTF **não garante automaticamente** o Certificado de Regularidade. Consulte:
  [Certificado de Regularidade no site do IBAMA](https://www.gov.br/ibama/pt-br/servicos/cadastros/ctf/certificado-de-regularidade#certificado-de-regularidade--cr-)

---

## ⚙️ Funcionalidades

- 🔄 **Download automático** dos arquivos mais recentes do CNPJ e IBAMA
- 📁 **Organização estruturada** dos dados por categoria:
  - `Dados CNPJ Estab/` – dados dos estabelecimentos
  - `Dados CNPJ Empresas/` – dados das matrizes
  - `Dados CTF IBAMA/` – dados do CTF por estado
- 🛠️ **Transformação automatizada**:
  - `estabelecimentos.csv`: CNPJ completo, nome fantasia, endereço, contatos
  - `cnae_estabelecimentos.csv`: CNPJ completo + CNAE primário e secundários (um por linha)
  - `dados_empresa.csv`: CNPJ básico, razão social, natureza jurídica, porte e capital social
  - `ctf_empresas.csv`: CNPJ completo + código da atividade (CTF)
  - `naturezas_juridicas.csv`: Códigos e descrições das naturezas jurídicas
- 📊 **Pronto para uso no Power BI** – cada arquivo pode ser facilmente importado e relacionado via CNPJ básico ou completo
  - Formato Parquet e Integração com Power BI
  - Além dos arquivos `.csv`, o pipeline também converte automaticamente todos os dados para o formato `.parquet`, que é mais eficiente para o Power BI por ser compactado e colunar.
  - Benefícios:
     🚀 Carregamento mais rápido no Power BI
     📉 Redução no tamanho dos arquivos
     📊 Melhor compatibilidade com grandes volumes de dados
  - Caminho salvo automaticamente: Após baixar e transformar os arquivos csv, os arquivos são salvos no caminho absoluto da pasta `Dados Painel Parquet`.

---

## 📁 Estrutura esperada de diretórios e arquivos

```bash
.
├── Dados CNPJ Estab/
├── Dados CNPJ Empresas/
├── Dados CTF IBAMA/
├── Entrada do Painel/
│   ├── estabelecimentos.csv
│   ├── cnae_estabelecimentos.csv
│   ├── dados_empresa.csv
│   ├── ctf_empresas.csv
│   └── naturezas_juridicas.csv
├── Dados Painel Parquet/
│   ├── estabelecimentos.parquet
│   ├── cnae_estabelecimentos.parquet
│   ├── dados_empresa.parquet
│   ├── ctf_empresas.parquet
│   └── naturezas_juridicas.parquet
├── caminho_dados_parquet.txt
├── get_files_online.py
├── transform_cnpj_estabelecimentos.py
├── transform_cnpj_empresas.py
├── transform_ctf.py
├── transform_natureza_juridica.py
├── run.py
├── requirements.txt
├── setup_and_run.py
├── export_to_parquet.py

```

---

## 🔹 Descrição dos Principais Arquivos

| Arquivo | Função |
|:---|:---|
| `get_files_online.py` | Faz o download automático dos arquivos da Receita Federal e do IBAMA, extrai arquivos ZIP e organiza no diretório correto. |
| `transform_cnpj_estabelecimentos.py` | Processa os dados dos estabelecimentos: gera `estabelecimentos.csv` e `cnae_estabelecimentos.csv`. |
| `transform_cnpj_empresas.py` | Processa os dados das matrizes: gera `dados_empresa.csv` com razão social, natureza jurídica, capital social e porte. |
| `transform_ctf.py` | Processa os dados de pessoas jurídicas do Cadastro Técnico Federal (CTF/APP) do IBAMA: gera `ctf_empresas.csv`. |
| `transform_natureza_juridica.py` | Processa os dados de naturezas jurídicas: gera `naturezas_juridicas.csv`. |
| `transform_cnae.py` | Processa a tabela oficial de CNAEs e gera `cnaes.csv`. |
| `export_to_parquet.py` | Script para conversão de todos os arquivos CSV da pasta de origem para Parquet, salvando na pasta de destino. |
| `caminho_dados_parquet.txt` | Documento gerado pelo pipeline com o caminho da pasta com os arquivos Parquet, para ser utilizado como parâmetro no Power BI e atualizar automaticamente as referências dos arquivos. |
| `run.py` | Script principal que executa todas as etapas em sequência: download, transformação e geração dos arquivos finais. |
| `setup_and_run.py` | Script de instalação e/ou atualização de pacotes e execução automática do pipeline. |
| `requirements.txt` | Lista dos pacotes Python necessários para execução. |

---

## 🚀 Como executar

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/Consulta-CTF-CNAE.git
```

2. Instale as dependências necessárias:
```bash
pip install -r requirements.txt
```

3. Execute o pipeline principal:
```bash
python run.py
```
  
Ou execute diretamente o instalador e executor automático:
```bash
python setup_and_run.py
```

---

## 📄 Licença

Este projeto é livre para fins educacionais e administrativos públicos, respeitando as políticas de uso dos dados da Receita Federal e do IBAMA.

---

## 🛠️ Observação Final

O repositório foi estruturado para facilitar futuras adaptações, como integração com bases de certificação ambiental ou cruzamento com outros cadastros governamentais.
