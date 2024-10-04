import os
from fastapi import FastAPI, Request, Form, Response
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
from database.database import execute_query, db_connection_check
from database.query import GET_ALL_USERS, GET_DATAS_PER_ONE_DAY, CONFIRM_DATA_AMOUNT, HEALTH_CHECK
# from app.slack import send_slack_csv
from psycopg.rows import dict_row
import logging
import uvicorn
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime

load_dotenv()

app = FastAPI()

slack_token = os.getenv("SLACK_BOT_TOKEN")
client = WebClient(token=slack_token)

# 고정 변수
chunk_size = 10


async def get_channel_id(request):
    form_data = await request.form()
    channel_id = form_data.get("channel_id")

    return channel_id


@app.post("/check/db")
async def check_db(request: Request):
    try:
    
        result = execute_query(GET_ALL_USERS)

        if result is None:
            return {"text": "데이터를 가져오는 중 오류가 발생했습니다.", "response_type": "in_channel"}
        
        print(f"{result}")

        return {"text": result, "response_type": "in_channel"}

    except Exception as e:
        logging.error(f"ERROR : {str(e)}")

@app.post("/check/users")
async def check_user(request: Request):
    try:
        
        result = execute_query(GET_ALL_USERS)

        if result is None:
            return {"text": "데이터를 가져오는 중 오류가 발생했습니다.", "response_type": "in_channel"}

        file_path = 'users.csv'
        result.to_csv(file_path, index=False)
        
        try:
            response = client.files_upload_v2(
                channel=await get_channel_id(request),  # 업로드할 채널 ID
                title="Result CSV file",
                file=file_path,  # 업로드할 파일 경로
                initial_comment=file_path
            )
            # 업로드 성공 시 응답 메시지 Slack API 결과를 그대로 리턴하지 않음
            return Response(status_code=200)
        except Exception as e:
            print(f"Slack API 에러: {str(e)}")
            return {"text": "Slack 파일 업로드 중 오류가 발생했습니다."}

    except Exception as e:
        logging.error(f"csv ERROR : {str(e)}")


def send_slack_csv(channel_id, file_path):
    try:
        client.files_upload_v2(
                channel=channel_id,  # 업로드할 채널 ID
                title="Result CSV file",
                file=file_path,  # 업로드할 파일 경로
                initial_comment=file_path
            )
    except SlackApiError as e:
        logging.error(f"Slack API Error : {str(e)}")
        return {"text": "파일 업로드 중 오류가 발생했습니다.", "response_type": "in_channel"}

# --start
@app.post("/check/db/amount")
async def confirm_data_amount(request: Request):
    try:
    
        result = execute_query(CONFIRM_DATA_AMOUNT)

        if result is None:
            return {"text": "데이터를 가져오는 중 오류가 발생했습니다.", "response_type": "in_channel"}
        
        file_path = 'db_results.csv'
        result.to_csv(file_path, index=False)
        send_slack_csv(await get_channel_id(request), file_path)

        return Response(status_code=200)

    except Exception as e:
        logging.error(f"ERROR : {str(e)}")
        

@app.post("/check/db/connection")
async def check_connection_to_db(request: Request):
    try:
        result = db_connection_check()
        print(result)

        if result:
            return {"text" : "데이터베이스 정상 작동 중입니다.", "response_type": "in_channel"}
        else:
            return {"text" : "데이터베이스에 문제가 발생했습니다.", "response_type": "in_channel"}
    except Exception as e:
        logging.error(f"ERROR : {str(e)}")


@app.post("/healthcheck")
async def healthcheck(request: Request):
    try:
        result = execute_query(HEALTH_CHECK)

        if result is None:
            return {"text": "데이터를 가져오는 중 오류가 발생했습니다.", "response_type": "in_channel"}
        
        file_path = 'healthcheck.csv'
        result.to_csv(file_path, index=False)
        send_slack_csv(await get_channel_id(request), file_path)

        return Response(status_code=200)

    except Exception as e:
        logging.error(f"ERROR : {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
