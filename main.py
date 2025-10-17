# ... (начало кода, импорты, конфигурация - все без изменений)
import asyncio
import logging
import os
import sys
from os import getenv

import asyncpg
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, CallbackQuery, BotCommand
)
from dotenv import load_dotenv

load_dotenv()
TOKEN = getenv("BOT_TOKEN")
ADMIN_ID = int(getenv("ADMIN_ID"))
DB_DSN = f"postgresql://{getenv('DB_USER')}:{getenv('DB_PASS')}@{getenv('DB_HOST')}:{getenv('DB_PORT')}/{getenv('DB_NAME')}"
COMPLAINT_THRESHOLD = 5
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)


class Database:
    def __init__(self, dsn):
        self.pool = None
        self.dsn = dsn

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, ssl='require')

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            # ИСПРАВЛЕНИЕ ЗДЕСЬ: "current_role" в кавычках
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    telegram_username TEXT,
                    registration_date TIMESTAMP DEFAULT NOW(),
                    status TEXT DEFAULT 'active',
                    complaint_count INTEGER DEFAULT 0,
                    "current_role" TEXT
                );
            """)
            # ... остальной код создания таблиц, который теперь соответствует SQL-скрипту
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS master_profiles (
                    profile_id SERIAL PRIMARY KEY, user_id BIGINT REFERENCES users(user_id) UNIQUE, name TEXT,
                    profile_photo_id TEXT, skills_description TEXT, service_area TEXT, is_active BOOLEAN DEFAULT TRUE
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS client_tasks (
                    task_id SERIAL PRIMARY KEY, user_id BIGINT REFERENCES users(user_id), task_description TEXT,
                    task_photo_id TEXT, location TEXT, status TEXT DEFAULT 'open', creation_date TIMESTAMP DEFAULT NOW()
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS master_offers (
                    offer_id SERIAL PRIMARY KEY, task_id INTEGER REFERENCES client_tasks(task_id),
                    master_user_id BIGINT REFERENCES users(user_id), offer_price TEXT, offer_message TEXT,
                    creation_date TIMESTAMP DEFAULT NOW(), UNIQUE(task_id, master_user_id)
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ads (
                    ad_id SERIAL PRIMARY KEY, ad_text TEXT NOT NULL, photo_id TEXT, button_text TEXT, button_url TEXT,
                    target_views INTEGER NOT NULL, current_views INTEGER DEFAULT 0, is_active BOOLEAN DEFAULT FALSE,
                    creation_date TIMESTAMP DEFAULT NOW()
                );
            """)
            logging.info("Таблицы базы данных проверены/созданы.")

    async def add_user(self, user_id, username):
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO users (user_id, telegram_username) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET telegram_username = $2", user_id, username)

    async def set_role(self, user_id, role):
        async with self.pool.acquire() as conn:
            # ИСПРАВЛЕНИЕ ЗДЕСЬ: "current_role" в кавычках
            await conn.execute('UPDATE users SET "current_role" = $1 WHERE user_id = $2', role, user_id)

    async def get_user(self, user_id):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

db = Database(DB_DSN)
class ClientTaskFSM(StatesGroup): description, photo, location = State(), State(), State()
class MasterProfileFSM(StatesGroup): name, skills, area = State(), State(), State()
class MasterSearchFSM(StatesGroup): waiting_for_keywords, browsing = State(), State()
class MasterOfferFSM(StatesGroup): price, message = State(), State()
class AdCreationFSM(StatesGroup): text, photo, button_text, button_url, target_views = State(), State(), State(), State(), State()

def get_role_kb(): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="👤 Я Заказчик"), KeyboardButton(text="🛠 Я Мастер")]], resize_keyboard=True)
def get_client_menu(): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="➕ Создать задачу")], [KeyboardButton(text="📂 Мои задачи"), KeyboardButton(text="🔄 Сменить роль")]], resize_keyboard=True)
def get_master_menu(): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🔍 Поиск задач")], [KeyboardButton(text="📝 Мой профиль"), KeyboardButton(text="🔄 Сменить роль")]], resize_keyboard=True)
def get_skip_kb(text="Пропустить"): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=text)]], resize_keyboard=True, one_time_keyboard=True)
def get_admin_kb(): return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="➕ Создать рекламу"), KeyboardButton(text="📊 Статус рекламы")]], resize_keyboard=True)

async def set_main_menu(bot: Bot):
    main_menu_commands = [
        BotCommand(command='/start', description='🚀 Запуск / Смена роли'),
        BotCommand(command='/menu', description='🏠 Главное меню'),
        BotCommand(command='/help', description='ℹ️ Помощь'),
        BotCommand(command='/admin', description='👑 Админ-панель')
    ]
    await bot.set_my_commands(main_menu_commands)

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = message.from_user
    await db.add_user(user.id, user.username)
    welcome_text = f"Привет, {user.first_name}! Добро пожаловать в <b>ДелоФикс</b>.\nКто вы?"
    await message.answer(welcome_text, reply_markup=get_role_kb())

@router.message(F.text.in_({"👤 Я Заказчик", "🛠 Я Мастер", "🔄 Сменить роль"}))
async def set_role(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    if "Заказчик" in message.text:
        await db.set_role(user_id, "client")
        await message.answer("Вы вошли как <b>Заказчик</b>.", reply_markup=get_client_menu())
    elif "Мастер" in message.text:
        await db.set_role(user_id, "master")
        await message.answer("Вы вошли как <b>Мастер</b>.", reply_markup=get_master_menu())

@router.message(Command("menu"))
async def cmd_menu(message: Message, state: FSMContext):
    await state.clear()
    user = await db.get_user(message.from_user.id)
    if not user: return await cmd_start(message, state)
    
    # ИСПРАВЛЕНИЕ ЗДЕСЬ: ключ словаря будет без кавычек
    role = user['current_role']
    if role == 'client': await message.answer("Меню Заказчика:", reply_markup=get_client_menu())
    elif role == 'master': await message.answer("Меню Мастера:", reply_markup=get_master_menu())
    else: await message.answer("Выберите роль:", reply_markup=get_role_kb())

# ... (остальной код остается таким же, так как он не обращается к 'current_role' напрямую)
# ... копипаст остального кода из предыдущего ответа ...
@router.message(Command("help"))
async def cmd_help(message: Message):
    help_text = "<b>ДелоФикс</b> - сервис для поиска мастеров и заказчиков.\n\n"\
                "<b>Как заказчик:</b> создавайте задачи, получайте отклики от мастеров и выбирайте лучшего.\n"\
                "<b>Как мастер:</b> заполните профиль и ищите релевантные задачи для отклика.\n\n"\
                "Для навигации используйте кнопки внизу или команду /menu."
    await message.answer(help_text)

@router.message(F.text == "➕ Создать задачу", StateFilter(None))
async def start_new_task(message: Message, state: FSMContext):
    await state.set_state(ClientTaskFSM.description)
    await message.answer("Шаг 1/3. Опишите вашу задачу.", reply_markup=types.ReplyKeyboardRemove())
@router.message(ClientTaskFSM.description, F.text)
async def task_desc_step(message: Message, state: FSMContext):
    await state.update_data(description=message.text)
    await state.set_state(ClientTaskFSM.photo)
    await message.answer("Шаг 2/3. Прикрепите фото проблемы (если нужно).", reply_markup=get_skip_kb())
@router.message(ClientTaskFSM.photo, F.photo)
async def task_photo_step(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await state.set_state(ClientTaskFSM.location)
    await message.answer("Шаг 3/3. Укажите район или город.", reply_markup=types.ReplyKeyboardRemove())
@router.message(ClientTaskFSM.photo, F.text == "Пропустить")
async def task_photo_skip(message: Message, state: FSMContext):
    await state.update_data(photo_id=None)
    await state.set_state(ClientTaskFSM.location)
    await message.answer("Шаг 3/3. Укажите район или город.", reply_markup=types.ReplyKeyboardRemove())
@router.message(ClientTaskFSM.location, F.text)
async def task_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("INSERT INTO client_tasks (user_id, task_description, task_photo_id, location) VALUES ($1, $2, $3, $4)",
                             message.from_user.id, data['description'], data.get('photo_id'), message.text)
    await state.clear()
    await message.answer("✅ Заявка опубликована!", reply_markup=get_client_menu())

@router.message(F.text == "📝 Мой профиль", StateFilter(None))
async def fill_profile(message: Message, state: FSMContext):
    await state.set_state(MasterProfileFSM.name)
    await message.answer("Заполним анкету. Как вас зовут?", reply_markup=types.ReplyKeyboardRemove())
@router.message(MasterProfileFSM.name, F.text)
async def prof_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await state.set_state(MasterProfileFSM.skills)
    await message.answer("Опишите ваши навыки и услуги.")
@router.message(MasterProfileFSM.skills, F.text)
async def prof_skills(message: Message, state: FSMContext):
    await state.update_data(skills=message.text)
    await state.set_state(MasterProfileFSM.area)
    await message.answer("Укажите районы, где вы работаете.")
@router.message(MasterProfileFSM.area, F.text)
async def prof_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO master_profiles (user_id, name, skills_description, service_area) VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET name = $2, skills_description = $3, service_area = $4, is_active = TRUE
        """, message.from_user.id, data['name'], data['skills'], message.text)
    await state.clear()
    await message.answer("✅ Профиль сохранен!", reply_markup=get_master_menu())

async def show_active_ad(message: Message):
    async with db.pool.acquire() as conn:
        ad = await conn.fetchrow("""
            SELECT ad_id, ad_text, photo_id, button_text, button_url FROM ads
            WHERE is_active = TRUE AND current_views < target_views
            ORDER BY creation_date DESC LIMIT 1
        """)
    if ad:
        kb = None
        if ad['button_text'] and ad['button_url']:
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=ad['button_text'], url=ad['button_url'])]])
        if ad['photo_id']: await message.answer_photo(photo=ad['photo_id'], caption=ad['ad_text'], reply_markup=kb)
        else: await message.answer(ad['ad_text'], reply_markup=kb, disable_web_page_preview=False)
        async with db.pool.acquire() as conn:
            await conn.execute("UPDATE ads SET current_views = current_views + 1 WHERE ad_id = $1", ad['ad_id'])

@router.message(F.text == "🔍 Поиск задач", StateFilter(None))
async def search_start(message: Message, state: FSMContext):
    await state.set_state(MasterSearchFSM.waiting_for_keywords)
    await message.answer("Введите поисковый запрос.", reply_markup=types.ReplyKeyboardRemove())
@router.message(MasterSearchFSM.waiting_for_keywords, F.text)
async def search_process(message: Message, state: FSMContext):
    query = message.text
    sql = "SELECT task_id FROM client_tasks WHERE status = 'open' AND to_tsvector('russian', task_description || ' ' || location) @@ plainto_tsquery('russian', $1) ORDER BY creation_date DESC LIMIT 50"
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(sql, query)
    if not rows:
        await message.answer("Ничего не найдено.", reply_markup=get_master_menu())
        await state.clear()
        return
    task_ids = [row['task_id'] for row in rows]
    await state.update_data(found_tasks=task_ids, current_index=0)
    await state.set_state(MasterSearchFSM.browsing)
    await message.answer(f"Найдено задач: {len(task_ids)}.", reply_markup=get_master_menu())
    await show_task(message, state, task_ids[0])
    await asyncio.sleep(1)
    await show_active_ad(message)

@router.message(Command("admin"), F.from_user.id == ADMIN_ID)
async def admin_panel(message: Message):
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=get_admin_kb())
@router.message(F.text == "➕ Создать рекламу", F.from_user.id == ADMIN_ID)
async def create_ad_start(message: Message, state: FSMContext):
    await state.set_state(AdCreationFSM.text)
    await message.answer("Шаг 1/5. Введите текст рекламного сообщения.", reply_markup=types.ReplyKeyboardRemove())
@router.message(AdCreationFSM.text, F.text)
async def ad_text_step(message: Message, state: FSMContext):
    await state.update_data(text=message.text)
    await state.set_state(AdCreationFSM.photo)
    await message.answer("Шаг 2/5. Прикрепите фото для рекламы.", reply_markup=get_skip_kb())
@router.message(AdCreationFSM.photo, F.photo)
async def ad_photo_step(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await state.set_state(AdCreationFSM.button_text)
    await message.answer("Шаг 3/5. Введите текст для кнопки-ссылки.", reply_markup=get_skip_kb("Без кнопки"))
@router.message(AdCreationFSM.photo, F.text == "Пропустить")
async def ad_photo_skip(message: Message, state: FSMContext):
    await state.update_data(photo_id=None)
    await state.set_state(AdCreationFSM.button_text)
    await message.answer("Шаг 3/5. Введите текст для кнопки-ссылки.", reply_markup=get_skip_kb("Без кнопки"))
@router.message(AdCreationFSM.button_text, F.text == "Без кнопки")
async def ad_button_skip(message: Message, state: FSMContext):
    await state.update_data(button_text=None, button_url=None)
    await state.set_state(AdCreationFSM.target_views)
    await message.answer("Шаг 5/5. Введите желаемое количество показов (просто число).")
@router.message(AdCreationFSM.button_text, F.text)
async def ad_button_text_step(message: Message, state: FSMContext):
    await state.update_data(button_text=message.text)
    await state.set_state(AdCreationFSM.button_url)
    await message.answer("Шаг 4/5. Отправьте полную ссылку для кнопки (например, https://google.com).")
@router.message(AdCreationFSM.button_url, F.text.startswith("http"))
async def ad_button_url_step(message: Message, state: FSMContext):
    await state.update_data(button_url=message.text)
    await state.set_state(AdCreationFSM.target_views)
    await message.answer("Шаг 5/5. Введите желаемое количество показов (просто число).")
@router.message(AdCreationFSM.target_views, F.text.isdigit())
async def ad_finish(message: Message, state: FSMContext):
    await state.update_data(target_views=int(message.text))
    data = await state.get_data()
    async with db.pool.acquire() as conn:
        await conn.execute("""
            UPDATE ads SET is_active = FALSE;
            INSERT INTO ads (ad_text, photo_id, button_text, button_url, target_views, is_active)
            VALUES ($1, $2, $3, $4, $5, TRUE)
        """, data['text'], data.get('photo_id'), data.get('button_text'), data.get('button_url'), data['target_views'])
    await state.clear()
    await message.answer("✅ Реклама создана и активирована! Старые кампании выключены.", reply_markup=get_admin_kb())
@router.message(F.text == "📊 Статус рекламы", F.from_user.id == ADMIN_ID)
async def ad_status(message: Message):
    async with db.pool.acquire() as conn:
        ads_list = await conn.fetch("SELECT * FROM ads ORDER BY creation_date DESC")
    if not ads_list: return await message.answer("Рекламных кампаний еще не было.")
    response = "<b>Статистика рекламных кампаний:</b>\n\n"
    for ad in ads_list:
        status = "🟢 Активна" if ad['is_active'] and ad['current_views'] < ad['target_views'] else "🔴 Завершена"
        response += f"<b>ID: {ad['ad_id']}</b> | {status}\nТекст: {ad['ad_text'][:30]}...\nПросмотры: {ad['current_views']} / {ad['target_views']}\n\n"
    await message.answer(response)

@router.message(F.text == "📂 Мои задачи")
async def my_tasks(message: Message, state: FSMContext): await message.answer("Раздел в разработке.")
@router.callback_query(F.data.startswith("make_offer:"))
async def start_offer(callback: CallbackQuery, state: FSMContext): await callback.answer("Раздел в разработке.")
@router.callback_query(MasterSearchFSM.browsing, F.data == "next_task")
async def next_task_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tasks = data.get('found_tasks', [])
    idx = data.get('current_index', -1)
    if idx + 1 < len(tasks):
        new_idx = idx + 1
        await state.update_data(current_index=new_idx)
        try: await callback.message.delete()
        except Exception: pass
        await show_task(callback.message, state, tasks[new_idx])
    else:
        await callback.answer("Это была последняя задача.", show_alert=True)
        await state.clear()
        await callback.message.answer("Поиск завершен.", reply_markup=get_master_menu())
async def show_task(message: Message, state: FSMContext, task_id):
    async with db.pool.acquire() as conn:
        task = await conn.fetchrow("SELECT * FROM client_tasks WHERE task_id = $1", task_id)
    if task:
        text = f"<b>Задача #{task['task_id']}</b>\n\n{task['task_description']}\n\n📍 {task['location']}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Следующая", callback_data="next_task")],[InlineKeyboardButton(text="💬 Сделать предложение", callback_data=f"make_offer:{task_id}")]])
        if task['task_photo_id']: await message.answer_photo(task['task_photo_id'], caption=text, reply_markup=kb)
        else: await message.answer(text, reply_markup=kb)
    else: await message.answer("Задача не найдена.")

async def main():
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    try:
        await db.connect()
        await db.create_tables()
        logging.info("Подключение к PostgreSQL успешно.")
    except Exception as e:
        logging.error(f"Ошибка подключения к БД: {e}")
        return
    await set_main_menu(bot)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
