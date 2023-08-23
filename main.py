import os

import pyrogram.types
from dotenv import load_dotenv
import asyncio
import nest_asyncio
from datetime import datetime, timedelta
from loguru import logger
from sqlalchemy import create_engine, Column, String, DateTime, BigInteger, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
from pyrogram import Client, filters
load_dotenv()
nest_asyncio.apply()


# Конфигурация Pyrogram
API_ID = os.getenv('APP_ID')
API_HASH = os.getenv('APP_HASH')
BOT_TOKEN = os.getenv('TG_TOKEN')

app = Client("SBH_BOT", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

DATABASE_URL = "sqlite:///SBH.sqlite3"
Base = declarative_base()

class Client(Base):
    __tablename__ = "client"

    username = Column(String, primary_key=True)
    chat_id = Column(BigInteger)
    timestamp = Column(DateTime)

class MessageLog(Base):
    __tablename__ = "message_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(BigInteger)
    sender = Column(String)
    text = Column(String)
    timestamp = Column(DateTime)


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


async def log_message(message):
    db = SessionLocal()
    message_log = MessageLog(
        chat_id=message.chat.id,
        sender=message.from_user.username if message.from_user else "BOT",
        text=message.text or "MEDIA",
        timestamp=message.date,
    )
    db.add(message_log)
    db.commit()
    db.close()

# Функция для отправки фото
async def send_photo(message):
    with open("photos/1.jpg", "rb") as photo:
        msg = await app.send_photo(message.chat.id, photo, caption="Отправил Вам фото")
        await log_message(msg)

    logger.info(f"Sent photo to {msg.chat.username} in response to message: {message.text}")

# Функция для отправки сообщения через 10 минут
async def send_greeting(chat_id):
    await asyncio.sleep(6)
    msg = await app.send_message(chat_id, "Добрый день!")
    logger.info(f"Sent message {msg.text} to {msg.chat.id}")
    await log_message(msg)

# Функция для отправки сообщения через 90 минут
async def send_material(chat_id):
    await asyncio.sleep(9)
    msg = await app.send_message(chat_id, "Подготовила для вас материал")
    logger.info(f"Sent message '{msg.text}' to {msg.chat.id}")
    await log_message(msg)

# Функция для отправки сообщения через 120 минут
async def send_return(chat_id):
    await asyncio.sleep(12)
    db = SessionLocal()
    messages = db.query(MessageLog)\
        .filter(MessageLog.chat_id == chat_id)\
        .filter(MessageLog.sender == "BOT")\
        .filter(MessageLog.timestamp >= datetime.utcnow() - timedelta(hours=2))\
        .all()
    db.close()
    if not any("Хорошего дня" in message.text for message in messages):
        msg = await app.send_message(chat_id, "Скоро вернусь с новым материалом!")
        logger.info(f"Sent message '{msg.text}' to {msg.chat.id}")
        await log_message(msg)


@app.on_message(filters.command('users_today') & filters.private)
async def users_today_command(client, message):
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    db = SessionLocal()
    user_count = db.query(Client).filter(Client.timestamp >= today).count()
    db.close()
    msg = await message.reply(f"Зарегистрированных пользователей сегодня: {user_count}")
    logger.info(f"Sent message '{msg.text}' to {msg.chat.id}")
    await log_message(msg)


# Обработчик для всех входящих сообщений
@app.on_message(filters.private)
async def handle_message(client, message: pyrogram.types.Message):
    logger.info(f"Received message from {message.from_user.username}: {message.text}")
    await log_message(message)

    db = SessionLocal()
    client = db.query(Client).get(message.from_user.username)

    if not client:
        client = Client(
            username=message.from_user.username,
            chat_id=message.chat.id,
            timestamp=message.date
        )
        db.add(client)

        db.commit()
        await send_photo(message)
        asyncio.create_task(send_greeting(message.chat.id))
        asyncio.create_task(send_material(message.chat.id))
        asyncio.create_task(send_return(message.chat.id))
    db.close()


if __name__ == "__main__":
    logger.add("bot_log.log", rotation="1 day")
    asyncio.get_event_loop().run_until_complete(app.run())