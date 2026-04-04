import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv() # Đọc lại file .env vừa sửa

try:
    print("Đang khởi động LangChain...")
    llm = ChatOpenAI(model="gpt-4o-mini")
    response = llm.invoke("Say 'Hello Data Engineer!'")
    print(f"Thành công! AI trả lời: {response.content}")
except Exception as e:
    print(f"LangChain vẫn lỗi: {e}")