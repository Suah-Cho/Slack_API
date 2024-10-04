from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

slack_token = os.getenv("SLACK_BOT_TOKEN")
client = WebClient(token=slack_token)

async def get_channel_id(request):
    form_data = await request.form()
    channel_id = form_data.get("channel_id")

    return channel_id


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