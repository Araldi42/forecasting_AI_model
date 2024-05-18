import os
import sys
import logging
import asyncio
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.encoders import jsonable_encoder
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.Mongo import Mongo