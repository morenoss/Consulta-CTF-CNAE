# Consulta-CTF-CNAE (versão aprimorada)

Este repositório é uma evolução do projeto original desenvolvida pela ex analista do STJ **Daniele Firme** [danielefm/Consulta-CTF-CNAE](https://github.com/danielefm/Consulta-CTF-CNAE), com melhorias na performance, estruturação do pipeline e tratamento de grandes volumes de dados.

🔧 A versão atual foi desenvolvida por **Moreno Santiago**, analista judiciário da **Secretaria de Administração do Superior Tribunal de Justiça (STJ)**, integrando um pipeline em Python com um modelo analítico no Power BI.

Dentre os principais aprimoramentos, destacam-se:

- 🚀 Conversão eficiente para o formato Parquet, com suporte a arquivos muito grandes (como `estabelecimentos.csv`), utilizando leitura em *chunks* e escrita otimizada com PyArrow.
- 🧱 Pipeline modular mais robusto, com scripts reutilizáveis e estruturados por tipo de dado (CNPJ, CNAE, CTF).
- 📊 Maior integração com Power BI, com foco em performance e escalabilidade no consumo de dados públicos.
- 📂 Caminho do diretório Parquet salvo automaticamente em `caminho_dados_parquet.txt`, utilizado como parâmetro dinâmico no modelo Power BI `Painel Consulta CTF R1.pbit`.

> **Importante:** Após a execução do pipeline com `run.py`, abra o modelo `Painel Consulta CTF R1.pbit` no Power BI e **insira o caminho indicado no arquivo `caminho_dados_parquet.txt`**. Isso garantirá o carregamento automático dos dados no painel.

Análise e consolidação de dados públicos da Receita Federal e do IBAMA utilizando Python, com foco em aplicações práticas no planejamento de compras públicas sustentáveis.

## 🎯 Objetivo

Este repositório contém um pipeline em Python que cruza informações do Cadastro Nacional da Pessoa Jurídica (CNPJ) da Receita Federal com os registros do Cadastro Técnico Federal (CTF/APP) do IBAMA, com base na Classificação Nacional de Atividades Econômicas (CNAE).

O projeto tem como finalidade:

- Baixar e consolidar os dados abertos da Receita Federal e do IBAMA;
- Transformar os dados em arquivos otimizados no formato Parquet, adequados para análise em larga escala;
- Alimentar um painel Power BI (`Painel Consulta CTF R1.pbit`) para apoiar o planejamento de contratações sustentáveis no setor público, especialmente no âmbito do STJ.

O painel permite responder a perguntas como:

- _Qual a proporção de empresas inscritas no CTF/APP por setor econômico (CNAE)?_
- _Quais segmentos apresentam baixa adesão ao CTF/APP e podem representar risco de fracasso contratual?_
- _Qual a idade média dos estabelecimentos por atividade econômica e unidade da federação?_

---

## 🧠 Justificativa

A **Lei nº 14.133/2021** (Nova Lei de Licitações) estabelece o **desenvolvimento nacional sustentável** como um dos princípios da contratação pública.

Nesse contexto, o **Guia de Licitações Sustentáveis da AGU** orienta que, sempre que possível, se avalie a viabilidade de exigir o **Certificado de Regularidade Ambiental junto ao IBAMA**.

Este repositório foi concebido para:

- Consolidar dados públicos da Receita Federal e do IBAMA em uma base unificada;
- Permitir a avaliação empírica da viabilidade de exigências ambientais em licitações;
- **Subsidiar tecnicamente a Administração Pública** com evidências reais sobre o nível de adesão ao CTF/APP por setor econômico;
- **Prevenir exigências inexequíveis** e garantir o equilíbrio entre sustentabilidade, competitividade e economicidade nas contratações públicas.

O painel tem sido utilizado no STJ para instrução de decisões administrativas, com base no **Acórdão TCU nº 1666/2019 – Plenário**, que orienta os órgãos a avaliarem a real capacidade do mercado em atender exigências técnicas.


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
├── Dados CNPJ Estab/                   # Arquivos brutos dos estabelecimentos
├── Dados CNPJ Empresas/                # Arquivos brutos das empresas (matriz)
├── Dados CTF IBAMA/                    # Arquivos brutos do IBAMA (por UF)
├── Dados CNAE/                         # Arquivos brutos da tabela de CNAEs
├── Dados Natureza Jurídica/            # Arquivos brutos da tabela de Naturezas Jurídicas
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
├── caminho_dados_parquet.txt             # caminho onde foram salvos os dados .parquet para utilizar no modelo do Power BI
├── Painel Consulta CTF R1.pbit
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
| `get_files_online.py` | Realiza o download automatizado de arquivos da Receita Federal e do IBAMA, incluindo extração de arquivos ZIP e renomeações quando necessário. |
| `transform_cnpj_estabelecimentos.py` | Transforma os dados de estabelecimentos (ativos) em dois arquivos: `estabelecimentos.csv` e `cnae_estabelecimentos.csv`, com colunas estruturadas e separação dos CNAEs primário e secundários. |
| `transform_cnpj_empresas.py` | Processa os dados das empresas (matriz), gerando `dados_empresa.csv` com CNPJ, razão social, natureza jurídica, capital social e porte. |
| `transform_ctf.py` | Consolida os dados de pessoas jurídicas inscritas no Cadastro Técnico Federal de Atividades Potencialmente Poluidoras (CTF/APP), gerando `ctf_empresas.csv`. |
| `transform_natureza_juridica.py` | Converte o arquivo bruto de naturezas jurídicas da Receita em formato legível, gerando `naturezas_juridicas.csv`. |
| `transform_cnae.py` | Trata a tabela oficial de CNAEs (Classificação Nacional de Atividades Econômicas) e gera `cnaes.csv`. |
| `export_to_parquet.py` | Converte todos os arquivos `.csv` da pasta de saída em arquivos `.parquet`, otimizados para leitura no Power BI. |
| `caminho_dados_parquet.txt` | Contém o caminho completo onde os arquivos `.parquet` foram salvos. Esse caminho deve ser inserido no parâmetro `RaizDados` ao abrir o painel `.pbit` no Power BI. |
| `run.py` | Script principal que executa o pipeline completo: limpa as pastas temporárias, baixa os dados, processa os arquivos, converte para Parquet e gera o caminho para uso no Power BI. |
| `setup_and_run.py` | Automatiza a instalação das dependências e executa o `run.py`. Ideal para usuários que executam o projeto pela primeira vez. |
| `requirements.txt` | Lista os pacotes Python necessários para o ambiente do projeto. |
| `Painel Consulta CTF R1.pbit` | Modelo de relatório do Power BI. Ao abrir, insira o caminho contido em `caminho_dados_parquet.txt` no parâmetro `RaizDados` para carregar os dados. |


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

3. Execute diretamente o instalador e executor automático:
```bash
python setup_and_run.py
```
Ou execute o somente o pipeline principal:
```bash
python run.py
```

## 📊 Como utilizar o Painel Power BI

Após a execução do pipeline, os arquivos `.parquet` necessários para o painel estarão disponíveis na pasta `Dados Painel Parquet/`.

Para carregar os dados no Power BI, siga os passos abaixo:

1. Abra o arquivo `Painel Consulta CTF R1.pbit` no Power BI Desktop.
2. Ao ser solicitado, informe o caminho da pasta que contém os arquivos `.parquet`.  
   Este caminho é gerado automaticamente e salvo no arquivo `caminho_dados_parquet.txt`, na raiz do projeto.
3. Copie o caminho do arquivo `caminho_dados_parquet.txt` e cole na tela de parâmetro do Power BI, no campo `RaizDados`.
4. Clique em “Carregar” para importar os dados e visualizar os relatórios.

⚠️ **Importante:**  
Não renomeie os arquivos `.parquet` gerados nem altere sua estrutura. O painel espera os seguintes arquivos:

- `estabelecimentos.parquet`
- `cnae_estabelecimentos.parquet`
- `dados_empresa.parquet`
- `ctf_empresas.parquet`
- `naturezas_juridicas.parquet`
- `cnaes.parquet`

Esses arquivos devem estar dentro da pasta informada como `RaizDados`.


## 📄 Licença

Este projeto é livre para fins educacionais e administrativos públicos, respeitando as políticas de uso dos dados da Receita Federal e do IBAMA.

🔹 O pipeline de dados e o modelo de relatório no Power BI foram desenvolvidos pela **Secretaria de Administração do Superior Tribunal de Justiça (STJ)**.

🔹 Este repositório deve ser referenciado em conjunto com os seguintes projetos que serviram de base e inspiração:

- [danielefm/Consulta-CTF-CNAE](https://github.com/danielefm/Consulta-CTF-CNAE)
- [morenoss/Consulta-CTF-CNAE](https://github.com/morenoss/Consulta-CTF-CNAE)

A colaboração, reuso e aprimoramento deste projeto são incentivados, desde que respeitados os devidos créditos institucionais e autorais.


---

## 🛠️ Observação Final

O repositório foi estruturado para facilitar futuras adaptações, como integração com bases de certificação ambiental ou cruzamento com outros cadastros governamentais.

---

## 🔗 Projeto Original

Este repositório tem como base o trabalho desenvolvido em [danielefm/Consulta-CTF-CNAE](https://github.com/danielefm/Consulta-CTF-CNAE), que estruturou uma abordagem prática para extração e cruzamento de dados do CNPJ (Receita Federal) e do Cadastro Técnico Federal (IBAMA).

A presente versão amplia a robustez do projeto original, com foco especial na automação do pipeline, tratamento de grandes volumes de dados e compatibilidade com análises avançadas no Power BI.

