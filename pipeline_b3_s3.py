import os
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def baixar_csv_com_selenium(destino_dir="dados_b3"):
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    os.makedirs(destino_dir, exist_ok=True)

    options = Options()
    options.add_argument("--headless")
    prefs = {"download.default_directory": os.path.abspath(destino_dir)}
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options)
    driver.get(url)

    time.sleep(5)  # aguarda carregamento da página
    botao = driver.find_element(By.XPATH, "//a[contains(text(), 'Download')]")
    botao.click()

    time.sleep(5)  # aguarda download
    driver.quit()

    # encontra o arquivo CSV baixado
    for file in os.listdir(destino_dir):
        if file.endswith(".csv"):
            caminho_csv = os.path.join(destino_dir, file)
            print(f"✅ CSV baixado: {caminho_csv}")
            return caminho_csv

    raise FileNotFoundError("❌ CSV não foi baixado.")

def processar_csv_para_parquet(caminho_csv, destino_base="dados_b3"):
    # Lê o CSV ignorando a primeira linha (título)
    df = pd.read_csv(
        caminho_csv,
        sep=";",
        encoding="latin1",
        skiprows=1,
        names=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"],
        usecols=["Código", "Ação", "Tipo", "Qtde Teórica", "Part (%)"],
        on_bad_lines="skip"
    )

    # Remove linhas não relacionadas à tabela principal
    df = df[df["Código"].notna() & ~df["Código"].str.contains("Quantidade|Redutor", na=False)]

    # Adiciona partição por data
    df["data_extracao"] = datetime.today().strftime("%Y-%m-%d")

    # Salva em Parquet com partição
    destino_particao = os.path.join(destino_base, f"data_extracao={df['data_extracao'].iloc[0]}")
    os.makedirs(destino_particao, exist_ok=True)
    caminho_parquet = os.path.join(destino_particao, "pregao.parquet")
    pq.write_table(pa.Table.from_pandas(df), caminho_parquet)

    print(f"📁 Parquet salvo em: {caminho_parquet}")
    return destino_particao


def upload_to_s3(local_path, bucket_name, s3_prefix):
    # s3 = boto3.client('s3')
    #Mockei pra teste mas n é o ideal por segurança
    s3 = boto3.client(
    's3',
    aws_access_key_id='ASIAYS2NQ6HQSM3LHY3Q',
    aws_secret_access_key='QvukLJuIfFMC7nV2G6ghOvIv3ajljdDQMXD8e6iU',
    aws_session_token='IQoJb3JpZ2luX2VjEA0aCXVzLXdlc3QtMiJHMEUCIBA7wg7DLd/JDYprJvuAJowHDm443OeqwggT0o+L8jwaAiEAoCasIpq/ChwPPpvaIRdWskVEqJVs2TjB5dWffICuc3QqvQIIlv//////////ARAAGgw1OTAxODM3MjM0ODkiDAPERbFmTyLVXiwDFiqRAh2Rl1Pz9asQs13+lIwIp5dSBn/UrGvmw+kyH+e7JnrVNzo72OteJtldyLMutycn4lUVOLhXU9gygHpj5HVu28/qANYLs3vA9INpkP6VIiZi9J1w1fgAheLDwHiPwuFr5KTn5DB5rnHQxVJDve0tQJqh38y4Q1evsNItAt/oKrqR9c0o/LrRrXlICmd97aYead18jWOb0RxbF52M5TlLFdJUFfiVs1Owti8SHyxCsOb2KZX0OWJg6+Mwoq64iE5nzl1RW1qKtPkmBjEG9bArVBZXXT2EOV6w6/d1XOcWwwf4FMJVei2MnC/HMtSbOEZLkt1Jne+LbqwPSdJSqpM7dcqPtIjgzIIqMHPewhk0Ao1ykzCw/9vGBjqdAU6hFCVLTaFBTzoqxr6TEY6TopkHfUnoF3AzVIxMoBEJyGSviBQD7Orp9M3JOjf9wzSoENDmPaRwXfwCXSLb/a4A9tDx6UGGyMXCawW2Qdm2WYEHeRiFY/rYPcebVL5WOia/AQQ1M2YUJAmnn15i6hq4kTmpOMK5VpjMo0QSX4StlsJtuSagWe2qLkLiW6z7wuH4BLYay5th9iX6UoM=',  # use o token completo
    region_name='us-east-1'
)

    for root, dirs, files in os.walk(local_path):
        for file in files:
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, local_path)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            s3.upload_file(full_path, bucket_name, s3_key)
            print(f"☁️ Upload concluído: s3://{bucket_name}/{s3_key}")

def run_pipeline():
    print("🚀 Iniciando pipeline B3...")
    caminho_csv = baixar_csv_com_selenium()
    caminho_parquet = processar_csv_para_parquet(caminho_csv)
    upload_to_s3(caminho_parquet, bucket_name="meu-bucket-b3-academy", s3_prefix="b3/pregao")
    print("🏁 Pipeline finalizado com sucesso.")

if __name__ == "__main__":
    run_pipeline()
