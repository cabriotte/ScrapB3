# 📈 Scrap de Dados da B3 para a AWS

Este projeto automatiza a extração para processamento e análise dos dados da **Carteira do Dia do índice IBOV** da B3, utilizando:

- 🐍 Python 3.11  
- ☁️ AWS S3 para armazenamento  

---

## 📦 Principais dependências

```txt
pandas==2.2.1
requests==2.31.0
pyarrow==12.0.1
boto3==1.34.78
selenium==4.15.2

```

## 🚀 Funcionalidades

- Acessa a página oficial da B3 e extrai o link do CSV da Carteira do Dia  
- Faz o download do arquivo CSV dinamicamente  
- Processa os dados e converte para formato Parquet particionado por data  
- Realiza o upload para um bucket S3  
- Pronto para ser consultado via Glue + Athena

---

## 🧰 Requisitos

- Python 3.11  
- ChromeDriver instalado e no PATH  
- AWS 
- Bucket S3 criado (ex: `meu-bucket-b3-academy`)

---

## 📦 Instalação

Clone o repositório e instale os pacotes:

```bash
git clone https://github.com/seu-usuario/ScrapB3.git
cd ScrapB3

python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

pip install -r requirements.txt
