
# 📊 CSV Processor API

Uma API simples desenvolvida com **FastAPI** para baixar arquivos CSV do **Google Drive**, processar os dados, e armazená-los em um banco de dados **PostgreSQL**. O processamento é feito em background para não travar a API.

---

## 🚀 Funcionalidades

* ✅ Baixa arquivos CSV do Google Drive via ID do arquivo.
* ✅ Processa e normaliza os dados (converte colunas para minúsculo, adiciona `chat_id`, sanitiza nome da tabela).
* ✅ Salva os dados no PostgreSQL.
* ✅ Roda o processamento em background com FastAPI para evitar travamento da API.

---

## 📦 Requisitos

* Python 3.8+
* PostgreSQL rodando e acessível
* Credenciais do banco no `.env`

---

## 📁 Estrutura

```
.
├── main.py           # Código principal da API
├── .env.exemplo      # Variáveis de ambiente
└── README.md         # Documentação
```

---

## ⚙️ Instalação

1. **Clone o repositório:**

```bash
git clone https://github.com/seu-usuario/csv-processor-api.git
cd csv-processor-api
```

2. **Crie o ambiente virtual:**

```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
```

3. **Instale as dependências:**

```bash
pip install -r requirements.txt
```

> **Exemplo de `requirements.txt`:**
>
> ```txt
> fastapi
> uvicorn
> gdown
> python-dotenv
> pandas
> sqlalchemy
> psycopg2-binary
> ```

4. **Configure as variáveis de ambiente:**

Crie um arquivo `.env` com o conteúdo:

```env
POSTGRES_URI=postgresql://usuario:senha@localhost:5432/seubanco
```

---

## ▶️ Como rodar a API

```bash
uvicorn main:app --reload
```

## 📬 Endpoint

### `POST /process_csv`

**Body (JSON):**
```json
{
"fileId": "<ID do arquivo no Google Drive>",
"chatId": "<Identificador do chat>"
}
```

**Resposta:**
```json
{
"status": "processing",
"message": "O arquivo está sendo processado em background."
}
```

Inicia o processamento do CSV.

#### Exemplo de payload:

```json
{
  "fileId": "1abcDefGhijkLmnopQRstUvWXyZ",
  "chatId": "usuario123"
}
```

#### Retorno:

```json
{
  "status": "processing",
  "message": "O arquivo está sendo processado em background."
}
```

## 🛠️ O que acontece no backend?

1. A API recebe o `fileId` e `chatId`.
2. O arquivo CSV é baixado do Google Drive.
3. Os dados são processados:

   * Colunas convertidas para minúsculas
   * Adiciona a coluna `chat_id`
   * Nome da tabela sanitizado com base no `chatId`
4. Os dados são inseridos no banco PostgreSQL (substituindo, se já existir).


## 🧼 Sanitização do nome da tabela

A função `sanitize_table_name()` remove caracteres especiais e garante que o nome da tabela comece com uma letra. Exemplo:

| `chatId` original | Nome da tabela gerado |
| ----------------- | --------------------- |
| `usuario123`      | `usuario123`          |
| `123teste`        | `usuario_123teste`    |
| `nome.completo`   | `nome_completo`       |


## ✅ Exemplo de uso com `curl`

```bash
curl -X POST http://localhost:8000/process_csv \
     -H "Content-Type: application/json" \
     -d '{"fileId": "1abcDefGhijkLmnopQRstUvWXyZ", "chatId": "usuario123"}'
```


## 📌 Notas

* Certifique-se de que o arquivo CSV no Google Drive esteja **público** ou **compartilhável via link**.
* O nome da tabela será sobrescrito no banco se já existir.
