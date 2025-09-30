import csv
import os
import re

import gdown
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, UploadFile
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import create_engine

load_dotenv()
app = FastAPI()


class SendFileRequest(BaseModel):
    fileId: str
    chatId: str


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/process_csv")
def process_csv(request: SendFileRequest, background_tasks: BackgroundTasks):
    """Roda o processamento do CSV em background para não travar a API"""
    logger.info(
        f"Recebida requisição CSV. fileId={request.fileId}, chatId={request.chatId}"
    )
    background_tasks.add_task(
        process_and_store_drive_csv, request.fileId, request.chatId
    )
    return {
        "status": "processing",
        "texto": "O arquivo está sendo processado em background.",
    }


@app.post("/upload_csv_stream")
def upload_csv_stream(
    chat_id: str = Form(...),
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = None,
):
    """
    Faz upload de um CSV diretamente (sem Google Drive/Spreadsheet)
    e processa em background em chunks.
    """
    logger.info(
        f"Recebido upload de CSV. chat_id={chat_id}, filename={file.filename}"
    )

    tmp_path = f"/tmp/{file.filename}"
    mode = "w"
    for df in pd.read_csv(file.file, on_bad_lines="skip", chunksize=50_000):
        df.to_csv(
            tmp_path,
            mode=mode,
            index=False,
            header=not os.path.exists(tmp_path),
        )
        mode = "a"

    # Iniciar processamento em background
    background_tasks.add_task(
        process_and_store_uploaded_csv, tmp_path, chat_id
    )

    return {
        "status": "processing",
        "texto": "O arquivo enviado está sendo processado em background.",
    }


def sanitize_table_name(name: str) -> str:
    logger.debug(f"Sanitizando nome da tabela: {name}")
    name = re.sub(r"\W+", "_", name)
    if not re.match(r"^[a-zA-Z]", name):
        name = f"usuario_{name}"
    return name.lower()


def send_webhook(chat_id: str, text: str):
    webhook_url = os.getenv("WEBHOOK_URL")
    data = {"texto": text, "chat_id": chat_id}
    try:
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()
        logger.debug("Mensagem enviada com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao enviar mensagem para webhook: {e}")


def save_dataframe_to_db(df: pd.DataFrame, chat_id: str, replace: bool = True):
    """Salva um DataFrame no banco com o nome da tabela baseado no chat_id"""
    engine = create_engine(os.getenv("POSTGRES_URI"))
    table_name = sanitize_table_name(chat_id)

    df.columns = df.columns.str.lower()
    df["chat_id"] = chat_id

    if_exists = "replace" if replace else "append"
    df.to_sql(table_name, engine, index=False, if_exists=if_exists)

    logger.success(f"Tabela {table_name} salva no banco ({len(df)} linhas).")
    return table_name


def process_csv_generic(
    file_id: str, chat_id: str, fetch_df_fn, chunksize: int = 50_000
):
    """Processa CSV genérico, usando função fetch_df_fn para obter os dados"""
    logger.info(
        f"Iniciando processamento. chat_id={chat_id}, file_id={file_id}"
    )

    try:
        total_rows = 0
        first_chunk = True

        for df in fetch_df_fn(file_id, chunksize):
            table_name = save_dataframe_to_db(df, chat_id, replace=first_chunk)
            total_rows += len(df)
            first_chunk = False

        send_webhook(chat_id, text="Seu arquivo foi processado com sucesso!")
        logger.success(
            f"Processamento concluído para chat_id={chat_id}, tabela={table_name}, linhas={total_rows}"
        )
    except Exception as e:
        logger.exception(
            f"Erro ao processar arquivo. chat_id={chat_id} - Erro: {e}"
        )
        send_webhook(chat_id, text="Ocorreu um erro ao processar seu arquivo")


def fetch_drive_csv(file_id: str, chunksize: int):
    """Baixa CSV do Google Drive e retorna iterador de chunks"""
    url = f"https://drive.google.com/uc?id={file_id}"
    output = "/tmp/temp.csv"
    gdown.download(url, output, quiet=False)
    logger.success(f"Download concluído: {output}")

    try:
        for chunk in pd.read_csv(output, chunksize=chunksize):
            yield chunk
    finally:
        os.remove(output)


def fetch_spreadsheet(file_id: str, chunksize: int):
    """Lê CSV diretamente de uma Google Spreadsheet"""
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv"
    for chunk in pd.read_csv(url, chunksize=chunksize):
        yield chunk


def fetch_uploaded_csv(file_obj, chunksize: int):
    """Lê CSV enviado via upload (stream) e gera chunks"""
    for chunk in pd.read_csv(file_obj, chunksize=chunksize):
        yield chunk
    os.remove(file_obj)


def process_and_store_drive_csv(
    file_id: str, chat_id: str, chunksize: int = 50_000
):
    process_csv_generic(file_id, chat_id, fetch_drive_csv, chunksize)


def process_and_store_spreadsheet(
    file_id: str, chat_id: str, chunksize: int = 50_000
):
    process_csv_generic(file_id, chat_id, fetch_spreadsheet, chunksize)


def process_and_store_uploaded_csv(
    file_obj, chat_id: str, chunksize: int = 50_000
):
    process_csv_generic(file_obj, chat_id, fetch_uploaded_csv, chunksize)
