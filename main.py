import gdown
import pandas as pd
from sqlalchemy import create_engine
from fastapi import FastAPI, BackgroundTasks
from dotenv import load_dotenv
from pydantic import BaseModel
import os
import re

load_dotenv()
app = FastAPI()

class SendFileRequest(BaseModel):
    fileId: str
    chatId: str

@app.post("/process_csv")
def process_csv(request: SendFileRequest, background_tasks: BackgroundTasks):
    """Roda o processamento em background para não travar a API"""
    background_tasks.add_task(process_and_store, request.fileId, request.chatId)
    return {"status": "processing", "message": "O arquivo está sendo processado em background."}


def download_csv_from_drive(file_id: str) -> pd.DataFrame:
    """Baixa CSV do Google Drive"""
    url = f"https://drive.google.com/uc?id={file_id}"
    output = "/tmp/temp.csv"
    gdown.download(url, output, quiet=False)
    df = pd.read_csv(output)
    return df

def sanitize_table_name(name: str) -> str:
    # Remove tudo que não for letra, número ou underscore
    name = re.sub(r'\W+', '_', name)
    # Garante que o nome comece com uma letra
    if not re.match(r'^[a-zA-Z]', name):
        name = f"usuario_{name}"
    return name.lower()

def process_and_store(file_id: str, chat_id: str):
    """Processa CSV e salva no banco de dados"""
    engine = create_engine(os.getenv("POSTGRES_URI"))

    df = download_csv_from_drive(file_id=file_id)
    df.columns = df.columns.str.lower()
    df['chat_id'] = chat_id
    table_name = sanitize_table_name(chat_id)

    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"✅ Processamento concluído para chat_id={chat_id}")

