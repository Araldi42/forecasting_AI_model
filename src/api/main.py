import os
import sys
import logging
import asyncio
import uvicorn
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'prophet'))
from model import main as forecast

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
logging.basicConfig(filename=f"{CURRENT_DIR}/log/{datetime.now()}_error.log",level=logging.ERROR)

load_dotenv()

app = FastAPI()
api_keys = [os.getenv("API_KEY")]

# A API IRÁ FORNCECER OS DADOS PREDISTOS PELO MODELO, E DEVE APENAS RECEBER UMA REQUISIÇÃO GET PARA RETORNAR OS PONTOS PREDITOS DE 1 SEMANA

@app.get("/api/predict")
async def predict_data(request: Request):
    """PREDICT DATA

    Parameters
    ----------
    request : Request
        Request object
    Returns
    -------
    dict
        Predicted data
    """
    try:
        if request.headers["api_key"] not in api_keys:
           raise HTTPException(status_code=401, detail="API Key inválida")
        predict_data = forecast()
        predict_data = predict_data[predict_data['ds'] > "2024-05-17 11:00:00"] # TO DO: Change this to everyly last data in the dataset at the time of the request
        return jsonable_encoder(predict_data.to_dict(orient="records"))
    except Exception as e:
        logging.error(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

async def main():
    """MAIN FUNCTION

    Parameters
    ----------
    None
    Returns
    -------
    None
    """
    config = uvicorn.Config("main:app", port=5000, host='0.0.0.0', log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())