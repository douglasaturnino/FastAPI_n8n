import gdown
import pandas as pd
from sqlalchemy import create_engine
from fastapi import FastAPI, BackgroundTasks
from dotenv import load_dotenv
from pydantic import BaseModel
import os
import re
from loguru import logger

load_dotenv()
app = FastAPI()

class SendFileRequest(BaseModel):
    fileId: str
    chatId: str


@app.post("/process_csv")
def process_csv(request: SendFileRequest, background_tasks: BackgroundTasks):
    """Roda o processamento em background para não travar a API"""
    logger.info(f"Recebida requisição para processar arquivo. fileId={request.fileId}, chatId={request.chatId}")
    background_tasks.add_task(process_and_store, request.fileId, request.chatId)
    return {"status": "processing", "message": "O arquivo está sendo processado em background."}


def download_csv_from_drive(file_id: str) -> str:
    """Baixa CSV do Google Drive e retorna caminho do arquivo"""
    try:
        logger.info(f"Iniciando download do arquivo do Google Drive. fileId={file_id}")
        url = f"https://drive.google.com/uc?id={file_id}"
        output = "/tmp/temp.csv"
        gdown.download(url, output, quiet=False)
        logger.success(f"Download concluído com sucesso. Arquivo salvo em {output}")
        return output
    except Exception as e:
        logger.error(f"Erro ao baixar CSV do Google Drive: {e}")
        raise


def sanitize_table_name(name: str) -> str:
    logger.debug(f"Sanitizando nome da tabela: {name}")
    # Remove tudo que não for letra, número ou underscore
    name = re.sub(r'\W+', '_', name)
    # Garante que o nome comece com uma letra
    if not re.match(r'^[a-zA-Z]', name):
        name = f"usuario_{name}"
    table_name = name.lower()
    logger.debug(f"Nome da tabela sanitizado: {table_name}")
    return table_name


def process_and_store(file_id: str, chat_id: str, chunksize: int = 50_000):
    """Processa CSV em chunks e salva no banco de dados"""
    logger.info(f"Iniciando processamento do arquivo. chat_id={chat_id}, chunksize={chunksize}")

    try:
        engine = create_engine(os.getenv("POSTGRES_URI"))
        logger.debug("Conexão com o banco criada.")

        file_path = download_csv_from_drive(file_id=file_id)
        table_name = sanitize_table_name(chat_id)

        # Leitura em chunks
        first_chunk = True
        total_rows = 0

        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            logger.info(f"Lendo chunk com {len(chunk)} linhas.")
            
            # Normaliza colunas apenas uma vez
            chunk.columns = chunk.columns.str.lower()
            chunk['chat_id'] = chat_id

            # Define se cria a tabela ou apenas adiciona
            if_exists = "replace" if first_chunk else "append"

            chunk.to_sql(table_name, engine, index=False, if_exists=if_exists)
            logger.success(f"Chunk salvo no banco: {len(chunk)} linhas.")

            total_rows += len(chunk)
            first_chunk = False

        logger.success(f"✅ Processamento concluído para chat_id={chat_id}, tabela={table_name}, total de linhas={total_rows}")
    except Exception as e:
        logger.exception(f"Erro durante o processamento do arquivo. chat_id={chat_id} - Erro: {e}")
