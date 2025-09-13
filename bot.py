# ------------------- ИМПОРТЛАР -------------------
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import calendar
import math
import re
import logging
import os
from typing import Dict, List, Optional
import matplotlib.pyplot as plt
import pandas as pd
from io import BytesIO
import seaborn as sns
import numpy as np

import gspread
from google.oauth2.service_account import Credentials

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import json
from aiogram.types import WebAppInfo
from aiogram.types import Update
from aiohttp import web

# ------------------- ЛОГИРОВАНИЕ -------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ------------------- КОНФИГ -------------------
# Environment variables дан фойдаланиш
from dotenv import load_dotenv
load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN", "7980938543:AAGULbsnJYRKanRFqdV9EBenSc9ceB-RILM")
GOOGLE_KEY_FILE = os.getenv("GOOGLE_KEY_FILE", "eastern-clock-469408-n2-800ab6e6fd76.json")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "142jCderpzFBwHqJJjbDQW633XXh8UC4o-iZkjhheVWs")
REPORT_SHEET_NAME = os.getenv("REPORT_SHEET_NAME", "Иш режаси")
REPORT_SHEET_MONTH = os.getenv("REPORT_SHEET_MONTH", "Ойлик хисобот")
ORDERS_SHEET_NAME = os.getenv("ORDERS_SHEET_NAME", "Буюртмалар")
TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Tashkent"))
WORKING_DAYS_IN_MONTH = int(os.getenv("WORKING_DAYS_IN_MONTH", "25"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "1453431600"))
GROUP_ID = int(os.getenv("GROUP_ID", "-1003084892237"))
ORDERS_TOPIC_ID = int(os.getenv("ORDERS_TOPIC_ID", "284"))
PRODUCTION_TOPIC_ID = int(os.getenv("PRODUCTION_TOPIC_ID", "27"))
RECOGNITION_TOPIC_ID = int(os.getenv("RECOGNITION_TOPIC_ID", "32"))
LOW_PERCENT_TOPIC_ID = int(os.getenv("LOW_PERCENT_TOPIC_ID", "96"))

# ------------------- GOOGLE SHEETS -------------------
scope = ["https://spreadsheets.google.com/feeds", 
         "https://www.googleapis.com/auth/drive",
         "https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file(GOOGLE_KEY_FILE, scopes=scope)
gc = gspread.authorize(creds)

try:
    doc = gc.open_by_key(SPREADSHEET_ID)
    logger.info(f"✅ Google Sheets га уланди: {doc.title}")
except Exception as e:
    logger.error(f"❌ Google Sheets ҳужжатига уланишда хato: {e}")
    try:
        gc = gspread.authorize(creds)
        doc = gc.open_by_key(SPREADSHEET_ID)
        logger.info(f"✅ Google Sheets га уланди: {doc.title}")
    except Exception as e2:
        logger.error(f"❌ Google Sheets га уланиб бўлмади: {e2}")
        raise

# Sheetларни очиш ёки мавжудлигини текшириш
try:
    sheet_report = doc.worksheet(REPORT_SHEET_NAME)
    logger.info(f"✅ '{REPORT_SHEET_NAME}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_report = doc.add_worksheet(title=REPORT_SHEET_NAME, rows=1000, cols=20)
        logger.info(f"✅ '{REPORT_SHEET_NAME}' янги яратилди")
        
        headers = [
            "Сана", "Бичиш Иш", "Бичиш Ходим", 
            "Тасниф Дикимга", "Тасниф Печат", "Тасниф Вишивка", "Тасниф Ходим",
            "Тикув Иш", "Тикув Ходим", "Оёқчи Ходим",
            "Қадоқлаш Иш", "Қадоқлаш Ходим", "Хафталик килинган иш"
        ]
        sheet_report.append_row(headers)
        logger.info("✅ Сарлавҳалар қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

try:
    sheet_month = doc.worksheet(REPORT_SHEET_MONTH)
    logger.info(f"✅ '{REPORT_SHEET_MONTH}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_month = doc.add_worksheet(title=REPORT_SHEET_MONTH, rows=10, cols=10)
        logger.info(f"✅ '{REPORT_SHEET_MONTH}' янги яратилди")
        
        month_headers = ["Бўлим", "Ойлик Режа", "Жами Бажарилди", "Қолдиқ", "Қолдиқ Фоиз", "Бажарилди Фоиз", "Кунлик Режа"]
        sheet_month.append_row(month_headers)
        
        sections = ["Бичиш", "Тасниф", "Тикув", "Қадоқлаш"]
        for i, section in enumerate(sections, start=2):
            sheet_month.update(f'A{i}', section)
            monthly_plan = 70000 if section == "Бичиш" else 65000 if section == "Тасниф" else 60000 if section == "Тикув" else 57000
            sheet_month.update(f'B{i}', monthly_plan)
            daily_plan = monthly_plan / WORKING_DAYS_IN_MONTH
            sheet_month.update(f'G{i}', round(daily_plan, 2))
        logger.info("✅ Ойлик хисобот сарлавҳалари қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

try:
    sheet_orders = doc.worksheet(ORDERS_SHEET_NAME)
    logger.info(f"✅ '{ORDERS_SHEET_NAME}' топилди")
except gspread.exceptions.WorksheetNotFound:
    try:
        sheet_orders = doc.add_worksheet(title=ORDERS_SHEET_NAME, rows=100, cols=10)
        logger.info(f"✅ '{ORDERS_SHEET_NAME}' янги яратилди")
        
        order_headers = ["Сана", "Буюртма номи", "Умумий микдор", "Бажарилди", "Қолдиқ", "Бажарилди Фоиз", "Қолдиқ Фоиз", "Жунатиш санаси", "Қолган кунлар", "Бўлим"]
        sheet_orders.append_row(order_headers)
        logger.info("✅ Буюртмалар сарлавҳалари қўшилди")
    except Exception as e:
        logger.error(f"❌ Хato: {e}")
        raise

# ------------------- БОТ -------------------
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=storage)

# ------------------- FSM -------------------
class SectionStates(StatesGroup):
    ish_soni = State()
    hodim_soni = State()
    pechat = State()
    vishivka = State()
    dikimga = State()
    tikuv_ish = State()
    tikuv_hodim = State()
    oyoqchi_hodim = State()

class OrderStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_quantity = State()
    waiting_for_date = State()
    waiting_for_deadline = State()
    waiting_for_section = State()
    edit_order_name = State()
    edit_order_quantity = State()
    edit_order_done = State()
    edit_order_deadline = State()
    edit_order_section = State()

# FSM стейтларини янгилаш
class DailyWorkStates(StatesGroup):
    waiting_for_section = State()
    waiting_for_order = State()
    waiting_for_quantity = State()
    waiting_for_section_quantity = State()
    waiting_for_section_workers = State()
# ------------------- УТИЛЛАР -------------------
def is_admin(user_id):
    return user_id == ADMIN_ID

def parse_float(s):
    try:
        if isinstance(s, str):
            s = s.replace(',', '').replace(' ', '')
            if '.' in s:
                parts = s.split('.')
                if len(parts) == 2:
                    s = parts[0] + '.' + parts[1][:2]
                else:
                    s = parts[0]
            return float(s)
        return float(s)
    except:
        return 0.0

def parse_int(s):
    try:
        return int(float(s.replace(',', '').replace(' ', '')))
    except:
        return 0

def today_date_str():
    return datetime.now(TZ).strftime("%d.%m.%Y")

def get_remaining_workdays():
    today = datetime.now(TZ)
    year = today.year
    month = today.month
    
    last_day = datetime(year, month, calendar.monthrange(year, month)[1], tzinfo=TZ)
    
    remaining_days = 0
    current_day = today + timedelta(days=1)
    
    while current_day <= last_day:
        if current_day.weekday() != 6:
            remaining_days += 1
        current_day += timedelta(days=1)
    
    return min(remaining_days, WORKING_DAYS_IN_MONTH - get_current_workday_index())

def get_current_workday_index():
    today = datetime.now(TZ)
    day = today.day
    
    workday_count = 0
    current_day = datetime(today.year, today.month, 1, tzinfo=TZ)
    
    while current_day <= today:
        if current_day.weekday() != 6:
            workday_count += 1
        current_day += timedelta(days=1)
    
    return min(workday_count, WORKING_DAYS_IN_MONTH)

def get_week_start_end_dates():
    today = datetime.now(TZ)
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start.strftime("%d.%m.%Y"), end.strftime("%d.%m.%Y")

def get_week_number():
    today = datetime.now(TZ)
    return today.isocalendar()[1]

def find_today_row(sheet) -> int:
    try:
        colA = sheet.col_values(1)
        today = today_date_str()
        for i, v in enumerate(colA, start=1):
            if v.strip() == today:
                return i
        return 0
    except Exception as e:
        logger.error(f"❌ find_today_row хato: {e}")
        return 0

def find_week_rows(sheet):
    try:
        colA = sheet.col_values(1)
        week_start, week_end = get_week_start_end_dates()
        week_rows = []
        
        for i, v in enumerate(colA, start=1):
            try:
                row_date = datetime.strptime(v, "%d.%m.%Y").replace(tzinfo=TZ)
                start_date = datetime.strptime(week_start, "%d.%m.%Y").replace(tzinfo=TZ)
                end_date = datetime.strptime(week_end, "%d.%m.%Y").replace(tzinfo=TZ)
                
                if start_date <= row_date <= end_date:
                    week_rows.append(i)
            except:
                continue
                
        return week_rows
    except Exception as e:
        logger.error(f"❌ find_week_rows хato: {e}")
        return []

def append_or_update(sheet, values_by_index: dict):
    try:
        row_idx = find_today_row(sheet)
        
        if row_idx == 0:
            max_index = max(values_by_index.keys()) + 1
            row = [""] * max_index
            row[0] = today_date_str()
            
            for idx, val in values_by_index.items():
                row[idx] = str(val)
            sheet.append_row(row)
            logger.info(f"✅ Янги қатор қўшилди")
            return len(sheet.get_all_values())
        else:
            for idx, val in values_by_index.items():
                sheet.update_cell(row_idx, idx + 1, str(val))
            logger.info(f"✅ Мавжуд қатор янгиланди")
            return row_idx
    except Exception as e:
        logger.error(f"❌ append_or_update хato: {e}")
        return 0

def safe_val(row, idx):
    return parse_int(row[idx]) if idx < len(row) else 0

def calculate_percentage(part, whole):
    """Фоизни тўғри хисоблаш функцияси"""
    try:
        if whole == 0:
            return 0
        percentage = (part / whole) * 100
        # Ограничиваем значение от 0 до 100
        return max(0, min(100, round(percentage, 1)))
    except Exception as e:
        logger.error(f"❌ calculate_percentage хato: part={part}, whole={whole}, error={e}")
        return 0

def update_monthly_totals(section_name, daily_value):
    """Ойлик хисоботни янгилаш ва фоизларни тўғри хисоблаш"""
    try:
        section_names = sheet_month.col_values(1)
        row_idx = None
        
        for i, name in enumerate(section_names, start=1):
            if name.strip().lower() == section_name.lower():
                row_idx = i
                break
        
        if not row_idx:
            logger.error(f"❌ {section_name} бўлими ойлик хисоботда топилмади")
            return None
        
        # Жорий жамғармани олиш
        current_total = parse_float(sheet_month.cell(row_idx, 3).value)
        new_total = current_total + daily_value
        
        # Ойлик режани олиш
        monthly_plan = parse_float(sheet_month.cell(row_idx, 2).value)
        
        # Қолдиқ ва фоизларни хисоблаш
        remaining = max(0, monthly_plan - new_total)
        
        # Фоизларни тўғри хисоблаш
        percentage = calculate_percentage(new_total, monthly_plan)
        remaining_percentage = calculate_percentage(remaining, monthly_plan)
        
        # Маълумотларни янгилаш
        sheet_month.update_cell(row_idx, 3, new_total)
        sheet_month.update_cell(row_idx, 4, remaining)
        sheet_month.update_cell(row_idx, 5, f"{remaining_percentage:.1f}%")
        sheet_month.update_cell(row_idx, 6, f"{percentage:.1f}%")
        
        logger.info(f"✅ {section_name} ойлик хисобот янгиланди: {new_total} та (режанинг {percentage:.1f}%)")
        
        # 100% бажарилганда хабарлар
        if percentage >= 100:
            return f"🎉 {section_name} бўлими ойлик режани {percentage:.1f}% бажариб, режадан {new_total - monthly_plan} та ортиқ иш чиқарди!"
        
    except Exception as e:
        logger.error(f"❌ Ойлик хисоботни янгилашда хato: {e}")
    
    return None

def get_monthly_data():
    monthly_plans = {}
    try:
        section_names = sheet_month.col_values(1)
        monthly_values = sheet_month.col_values(2)
        monthly_done = sheet_month.col_values(3)
        monthly_remaining = sheet_month.col_values(4)
        monthly_remaining_pct = sheet_month.col_values(5)
        monthly_done_pct = sheet_month.col_values(6)
        daily_plans = sheet_month.col_values(7)
        
        for i, name in enumerate(section_names):
            if i > 0 and i < len(monthly_values):  # Пропускаем заголовок
                cleaned_name = name.strip().lower()
                
                # Получаем значения плана и выполнения
                plan = parse_float(monthly_values[i]) if i < len(monthly_values) else 0
                done = parse_float(monthly_done[i]) if i < len(monthly_done) else 0
                
                # Пересчитываем проценты для обеспечения точности
                done_pct = calculate_percentage(done, plan)
                remaining = max(0, plan - done)
                remaining_pct = calculate_percentage(remaining, plan)
                
                monthly_plans[cleaned_name] = {
                    'plan': plan,
                    'done': done,
                    'remaining': remaining,
                    'remaining_pct': f"{remaining_pct:.1f}%",
                    'done_pct': f"{done_pct:.1f}%",
                    'daily_plan': parse_float(daily_plans[i]) if i < len(daily_plans) else 0
                }
                
                logger.info(f"📊 {cleaned_name}: {done}/{plan} ({done_pct:.1f}%)")
    except Exception as e:
        logger.error(f"❌ get_monthly_data хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return monthly_plans

def calculate_section_performance(section_name, daily_value, monthly_plan_dict):
    total_workdays = WORKING_DAYS_IN_MONTH
    monthly_plan = monthly_plan_dict.get('plan', 0)
    daily_norm = monthly_plan / total_workdays if total_workdays > 0 else 0
    
    monthly_done = monthly_plan_dict.get('done', 0) + daily_value
    monthly_pct = calculate_percentage(monthly_done, monthly_plan)
    
    monthly_remaining = max(0, monthly_plan - monthly_done)
    monthly_remaining_pct = calculate_percentage(monthly_remaining, monthly_plan)
    
    remaining_days = get_remaining_workdays()
    daily_needed = monthly_remaining / remaining_days if remaining_days > 0 else 0
    
    daily_pct = calculate_percentage(daily_value, daily_norm)
    
    return {
        'daily_norm': daily_norm,
        'daily_pct': daily_pct,
        'monthly_pct': monthly_pct,
        'monthly_remaining_pct': monthly_remaining_pct,
        'remaining_work': monthly_remaining,
        'daily_needed': daily_needed,
        'remaining_days': remaining_days
    }

def get_month_name():
    months_uz = {
        1: "Январь", 2: "Февраль", 3: "Марть", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    return months_uz.get(datetime.now().month, "")

def get_orders_data():
    try:
        orders = []
        records = sheet_orders.get_all_values()
        
        for i in range(1, len(records)):
            if len(records[i]) >= 10 and records[i][0] and records[i][1]:
                order_date = records[i][0]
                order_name = records[i][1]
                total_qty = parse_float(records[i][2]) if len(records[i]) > 2 else 0
                done_qty = parse_float(records[i][3]) if len(records[i]) > 3 else 0
                remaining = parse_float(records[i][4]) if len(records[i]) > 4 else 0
                done_percentage = records[i][5] if len(records[i]) > 5 else "0%"
                remaining_percentage = records[i][6] if len(records[i]) > 6 else "0%"
                deadline = records[i][7] if len(records[i]) > 7 else ""
                days_left = records[i][8] if len(records[i]) > 8 else 0
                section = records[i][9] if len(records[i]) > 9 else ""
                
                orders.append({
                    'date': order_date,
                    'name': order_name,
                    'total': total_qty,
                    'done': done_qty,
                    'remaining': remaining,
                    'done_percentage': done_percentage,
                    'remaining_percentage': remaining_percentage,
                    'deadline': deadline,
                    'days_left': days_left,
                    'section': section,
                    'row_index': i + 1
                })
        
        return orders
    except Exception as e:
        logger.error(f"❌ Буюртмаларни ўқишda хato: {e}")
        return []

# get_orders_by_section funksiyasida
def get_orders_by_section(section_name):
    orders = get_orders_data()
    normalized_section = normalize_section_name(section_name).strip().lower()
    
    result = []
    for order in orders:
        order_section = normalize_section_name(order.get('section', '')).strip().lower()
        if order_section == normalized_section and order['remaining'] > 0:
            result.append(order)
    
    return result

def update_order_in_sheet(row_index, field, value):
    try:
        col_idx = 0
        if field == "done":
            col_idx = 3
        elif field == "total":
            col_idx = 2
        elif field == "deadline":
            col_idx = 7
        elif field == "name":
            col_idx = 1
        elif field == "section":
            col_idx = 9
        
        if col_idx > 0:
            sheet_orders.update_cell(row_index, col_idx + 1, str(value))
            
            if field in ["done", "total"]:
                row_values = sheet_orders.row_values(row_index)
                
                total = parse_float(value) if field == "total" else parse_float(row_values[2])
                done = parse_float(value) if field == "done" else parse_float(row_values[3])
                remaining = max(0, total - done)
                
                done_pct = calculate_percentage(done, total)
                remaining_pct = calculate_percentage(remaining, total)
                
                sheet_orders.update_cell(row_index, 5, remaining)
                sheet_orders.update_cell(row_index, 6, f"{done_pct:.1f}%")
                sheet_orders.update_cell(row_index, 7, f"{remaining_pct:.1f}%")
                
                deadline = row_values[7] if len(row_values) > 7 else ""
                if deadline:
                    try:
                        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
                        today = datetime.now(TZ)
                        days_left = (deadline_date - today).days
                        sheet_orders.update_cell(row_index, 9, days_left)
                    except:
                        pass
            
            logger.info(f"✅ Буюртма янгиланди: {field} = {value}")
            return True
        
        return False
    except Exception as e:
        logger.error(f"❌ Буюртмани янгилашda хato: {e}")
        return False

async def send_to_group(message_text, topic_id=None, parse_mode=None):
    try:
        message_thread_id = topic_id
        
        await bot.send_message(
            chat_id=GROUP_ID,
            text=message_text,
            message_thread_id=message_thread_id,
            parse_mode=parse_mode
        )
        logger.info(f"✅ Хабар гуруҳга жўнатилди (Topic: {message_thread_id})")
    except Exception as e:
        logger.error(f"❌ Гуруҳга хабар жўнатишda хato: {e}")

def validate_order_data(order_name, quantity, deadline):
    errors = []
    
    if not order_name or len(order_name.strip()) < 2:
        errors.append("❌ Буюртма номи энг камда 2 та ҳарфдан иборат бўлиши керак")
    
    try:
        quantity = int(quantity)
        if quantity <= 0:
            errors.append("❌ Миқдор мусбат сон бўлиши керак")
    except ValueError:
        errors.append("❌ Миқдорни нотоғри киритдинзи. Бутун сон киритинг")
    
    try:
        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
        today = datetime.now(TZ)
        if deadline_date <= today:
            errors.append("❌ Муддат бугундан кейинги сана бўлиши керак")
    except ValueError:
        errors.append("❌ Санани нотоғри форматда киритдингиз. Тўғри формат: кун.ой.йил")
    
    return errors

# ------------------- ГРАФИК ФУНКЦИЯЛАРИ -------------------
def create_percentage_pie_chart():
    """Pie chart для отображения процентов выполнения плана"""
    try:
        monthly_data = get_monthly_data()
        if not monthly_data:
            logger.error("❌ Monthly data is empty")
            return None
            
        sections = []
        percentages = []
        actual_values = []  # Фактические значения для отображения
        
        # Собираем данные только для существующих отделов
        for section_name in ['бичиш', 'тасниф', 'тикув', 'қадоқлаш']:
            if section_name in monthly_data:
                data = monthly_data[section_name]
                # Извлекаем числовое значение процента
                done_pct_str = data.get('done_pct', '0%')
                try:
                    # Удаляем символ % и преобразуем в число
                    pct_value = float(done_pct_str.replace('%', '').strip())
                    # Ограничиваем значение 100%
                    pct_value = min(100, max(0, pct_value))
                    
                    sections.append(section_name.capitalize())
                    percentages.append(pct_value)
                    actual_values.append(f"{data.get('done', 0):.0f}/{data.get('plan', 1):.0f}")
                except ValueError as e:
                    logger.error(f"❌ Error parsing percentage for {section_name}: {done_pct_str}, error: {e}")
                    continue
        
        if not sections:
            logger.error("❌ No valid sections data for pie chart")
            return None
        
        # Цвета для секторов
        colors = []
        for p in percentages:
            if p >= 100:
                colors.append('#4CAF50')  # Ярко-зеленый
            elif p >= 80:
                colors.append('#8BC34A')  # Светло-зеленый
            elif p >= 60:
                colors.append('#FFC107')  # Янтарный
            elif p >= 40:
                colors.append('#FF9800')  # Оранжевый
            else:
                colors.append('#F44336')  # Красный
        
        # Создаем pie chart
        plt.figure(figsize=(12, 10))
        
        # Explode сектор с наибольшим процентом
        explode = [0.05 if p == max(percentages) else 0 for p in percentages]
        
        wedges, texts, autotexts = plt.pie(
            percentages, 
            labels=sections, 
            colors=colors, 
            autopct='%1.1f%%', 
            startangle=90, 
            shadow=True,
            explode=explode,
            textprops={'fontsize': 12}
        )
        
        # Увеличиваем размер текста с процентами
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(14)
            autotext.set_fontweight('bold')
        
        # Добавляем легенду с фактическими значениями
        legend_labels = [f'{sect}: {val}' for sect, val in zip(sections, actual_values)]
        plt.legend(wedges, legend_labels, title="Бажарилди/Режа", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.axis('equal')
        plt.title('Ойлик режа бажарилиши фоизда\n', fontsize=16, fontweight='bold')
        
        # Добавляем общую информацию
        total_done = sum([float(p) for p in percentages]) / len(percentages) if percentages else 0
        plt.figtext(0.5, 0.01, f'Умумий бажарилди: {total_done:.1f}%', ha='center', fontsize=12)
        
        plt.tight_layout()
        
        # Сохраняем график
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        logger.info(f"✅ Pie chart created successfully with {len(sections)} sections")
        return buf
    except Exception as e:
        logger.error(f"❌ Фоизлар учун pie chart яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_weekly_trend_chart():
    """Недельная тенденция производства - текущая неделя"""
    try:
        today = datetime.now(TZ)
        start_of_week = today - timedelta(days=today.weekday())
        
        # Получаем данные за текущую неделю
        records = sheet_report.get_all_values()
        weekly_data = {}
        
        # Создаем все дни недели
        for i in range(7):
            current_date = start_of_week + timedelta(days=i)
            date_str = current_date.strftime("%d.%m.%Y")
            weekly_data[date_str] = {
                'bichish': 0,
                'tasnif': 0,
                'tikuv': 0,
                'qadoqlash': 0,
                'date_obj': current_date
            }
        
        # Заполняем данные из таблицы
        for row in records[1:]:
            if len(row) > 0 and row[0]:
                try:
                    if row[0] in weekly_data:
                        weekly_data[row[0]]['bichish'] = safe_val(row, 1)
                        weekly_data[row[0]]['tasnif'] = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                        weekly_data[row[0]]['tikuv'] = safe_val(row, 7)
                        weekly_data[row[0]]['qadoqlash'] = safe_val(row, 10)
                except:
                    continue
        
        # Сортируем данные по дате
        sorted_dates = sorted(weekly_data.keys())
        dates = [weekly_data[date]['date_obj'] for date in sorted_dates]
        
        # Подготовка данных для графиков
        bichish_values = [weekly_data[date]['bichish'] for date in sorted_dates]
        tasnif_values = [weekly_data[date]['tasnif'] for date in sorted_dates]
        tikuv_values = [weekly_data[date]['tikuv'] for date in sorted_dates]
        qadoqlash_values = [weekly_data[date]['qadoqlash'] for date in sorted_dates]
        
        # Создаем график с двумя subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
        
        # Линейный график
        ax1.plot(dates, bichish_values, marker='o', label='Бичиш', linewidth=2)
        ax1.plot(dates, tasnif_values, marker='o', label='Тасниф', linewidth=2)
        ax1.plot(dates, tikuv_values, marker='o', label='Тикув', linewidth=2)
        ax1.plot(dates, qadoqlash_values, marker='o', label='Қадоқлаш', linewidth=2)
        
        ax1.set_xlabel('Кунлар')
        ax1.set_ylabel('Иш микдори')
        ax1.set_title('Ҳафталик иш чиқими тенденцияси (линейный график)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Форматирование дат на оси X
        ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%a\n%d.%m'))
        ax1.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator())
        
        # Bar chart
        x = range(len(dates))
        width = 0.2
        
        ax2.bar([i - width*1.5 for i in x], bichish_values, width, label='Бичиш', color='skyblue')
        ax2.bar([i - width*0.5 for i in x], tasnif_values, width, label='Тасниф', color='lightgreen')
        ax2.bar([i + width*0.5 for i in x], tikuv_values, width, label='Тикув', color='lightcoral')
        ax2.bar([i + width*1.5 for i in x], qadoqlash_values, width, label='Қадоқлаш', color='gold')
        
        ax2.set_xlabel('Кунлар')
        ax2.set_ylabel('Иш микдори')
        ax2.set_title('Ҳафталик иш чиқими тенденцияси (столбчатая диаграмма)')
        ax2.set_xticks(x)
        ax2.set_xticklabels([date.strftime('%a\n%d.%m') for date in dates])
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Сохраняем график
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        
        return buf
    except Exception as e:
        logger.error(f"❌ Ҳафталик тенденция графиги яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_monthly_trend_chart():
    """Месячная тенденция производства - текущий месяц"""
    try:
        today = datetime.now(TZ)
        start_of_month = today.replace(day=1)
        
        # Определяем последний день месяца
        if today.month == 12:
            end_of_month = today.replace(day=31)
        else:
            end_of_month = today.replace(month=today.month+1, day=1) - timedelta(days=1)
        
        # Получаем данные за текущий месяц
        records = sheet_report.get_all_values()
        monthly_data = {}
        
        # Создаем все дни месяца
        current_date = start_of_month
        while current_date <= end_of_month:
            date_str = current_date.strftime("%d.%m.%Y")
            monthly_data[date_str] = {
                'bichish': 0,
                'tasnif': 0,
                'tikuv': 0,
                'qadoqlash': 0,
                'date_obj': current_date
            }
            current_date += timedelta(days=1)
        
        # Заполняем данные из таблицы
        for row in records[1:]:
            if len(row) > 0 and row[0]:
                try:
                    if row[0] in monthly_data:
                        monthly_data[row[0]]['bichish'] = safe_val(row, 1)
                        monthly_data[row[0]]['tasnif'] = safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
                        monthly_data[row[0]]['tikuv'] = safe_val(row, 7)
                        monthly_data[row[0]]['qadoqlash'] = safe_val(row, 10)
                except:
                    continue
        
        # Сортируем данные по дате
        sorted_dates = sorted(monthly_data.keys())
        dates = [monthly_data[date]['date_obj'] for date in sorted_dates]
        
        # Подготовка данных для графиков
        bichish_values = [monthly_data[date]['bichish'] for date in sorted_dates]
        tasnif_values = [monthly_data[date]['tasnif'] for date in sorted_dates]
        tikuv_values = [monthly_data[date]['tikuv'] for date in sorted_dates]
        qadoqlash_values = [monthly_data[date]['qadoqlash'] for date in sorted_dates]
        
        # Создаем график с двумя subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
        
        # Линейный график
        ax1.plot(dates, bichish_values, marker='o', label='Бичиш', linewidth=2)
        ax1.plot(dates, tasnif_values, marker='o', label='Тасниф', linewidth=2)
        ax1.plot(dates, tikuv_values, marker='o', label='Тикув', linewidth=2)
        ax1.plot(dates, qadoqlash_values, marker='o', label='Қадоқлаш', linewidth=2)
        
        ax1.set_xlabel('Кунлар')
        ax1.set_ylabel('Иш микдори')
        ax1.set_title('Ойлик иш чиқими тенденцияси (линейный график)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Форматирование дат на оси X
        ax1.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%d'))
        ax1.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator(interval=2))
        
        # Bar chart
        x = range(len(dates))
        width = 0.2
        
        ax2.bar([i - width*1.5 for i in x], bichish_values, width, label='Бичиш', color='skyblue')
        ax2.bar([i - width*0.5 for i in x], tasnif_values, width, label='Тасниф', color='lightgreen')
        ax2.bar([i + width*0.5 for i in x], tikuv_values, width, label='Тикув', color='lightcoral')
        ax2.bar([i + width*1.5 for i in x], qadoqlash_values, width, label='Қадоқлаш', color='gold')
        
        ax2.set_xlabel('Кунлар')
        ax2.set_ylabel('Иш микдори')
        ax2.set_title('Ойлик иш чиқими тенденцияси (столбчатая диаграмма)')
        ax2.set_xticks(x[::2])  # Показываем каждую вторую дату
        ax2.set_xticklabels([date.strftime('%d') for date in dates][::2])
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Добавляем название месяца под графиком
        month_name = get_month_name()
        plt.figtext(0.5, 0.01, f'{month_name}', ha='center', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.1)  # Оставляем место для названия месяца
        
        # Сохраняем график
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        buf.seek(0)
        plt.close()
        
        return buf
    except Exception as e:
        logger.error(f"❌ Ойлик тенденция графиги яратишда хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

# ------------------- ХИСОБОТ ФУНКЦИЯЛАРИ -------------------
def format_daily_report():
    try:
        row_idx = find_today_row(sheet_report)
        if row_idx == 0:
            return "❌ Бугун учун маълумотлар киритилмаган."
            
        row = sheet_report.row_values(row_idx)
        monthly_data = get_monthly_data()
        
        # Emoji lug'ati
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        report = f"📊 Кунлик хисобот ({row[0]})\n\n"
        
        # Бичиш
        bichish_ish = safe_val(row, 1)
        bichish_hodim = safe_val(row, 2)
        bichish_data = monthly_data.get('бичиш', {})
        bichish_performance = calculate_section_performance('бичиш', bichish_ish, bichish_data)
        
        report += f"{section_emojis.get('бичиш', '✂️')} Бичиш: {bichish_ish} та\n"
        report += f"      Ходимлар: {bichish_hodim}\n"
        report += f"      Кунлик норма: {bichish_performance['daily_norm']:.1f} та\n"
        report += f"      Кунлик фоиз: {bichish_performance['daily_pct']:.1f}%\n"
        report += f"      Ойлик фоиз: {bichish_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {bichish_performance['remaining_work']:.1f} та\n"
        report += f"      Ҳар кунги керак: {bichish_performance['daily_needed']:.1f} та/кун\n\n"
        
        # Тасниф
        tasnif_dikimga = safe_val(row, 3)
        tasnif_pechat = safe_val(row, 4)
        tasnif_vishivka = safe_val(row, 5)
        tasnif_hodim = safe_val(row, 6)
        tasnif_total = tasnif_dikimga + tasnif_pechat + tasnif_vishivka
        tasnif_data = monthly_data.get('тасниф', {})
        tasnif_performance = calculate_section_performance('тасниф', tasnif_total, tasnif_data)
        
        report += f"{section_emojis.get('тасниф', '📑')} Тасниф: {tasnif_total} та\n"
        report += f"      Ходимлар: {tasnif_hodim}\n"
        report += f"      Кунлик норма: {tasnif_performance['daily_norm']:.1f} та\n"
        report += f"      Кунлик фоиз: {tasnif_performance['daily_pct']:.1f}%\n"
        report += f"      Ойлик фоиз: {tasnif_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {tasnif_performance['remaining_work']:.1f} та\n"
        report += f"      Ҳар кунги керак: {tasnif_performance['daily_needed']:.1f} та/кун\n\n"
        
        # Тикув
        tikuv_ish = safe_val(row, 7)
        tikuv_hodim = safe_val(row, 8)
        oyoqchi_hodim = safe_val(row, 9)
        tikuv_data = monthly_data.get('тикув', {})
        tikuv_performance = calculate_section_performance('тикув', tikuv_ish, tikuv_data)
        
        report += f"{section_emojis.get('тикув', '🧵')} Тикув: {tikuv_ish} та\n"
        report += f"      Ходимлар: {tikuv_hodim}\n"
        report += f"      Кунлик норма: {tikuv_performance['daily_norm']:.1f} та\n"
        report += f"      Кунлик фоиз: {tikuv_performance['daily_pct']:.1f}%\n"
        report += f"      Ойлик фоиз: {tikuv_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {tikuv_performance['remaining_work']:.1f} та\n"
        report += f"      Ҳар кунги керак: {tikuv_performance['daily_needed']:.1f} та/кун\n\n"
        
        # Қадоқлаш
        qadoqlash_ish = safe_val(row, 10)
        qadoqlash_hodim = safe_val(row, 11)
        qadoqlash_data = monthly_data.get('қадоқлаш', {})
        qadoqlash_performance = calculate_section_performance('қадоқлаш', qadoqlash_ish, qadoqlash_data)
        
        report += f"{section_emojis.get('қадоқлаш', '📦')} Қадоқлаш: {qadoqlash_ish} та\n"
        report += f"      Ходимлар: {qadoqlash_hodim}\n"
        report += f"      Кунлик норма: {qadoqlash_performance['daily_norm']:.1f} та\n"
        report += f"      Кунлик фоиз: {qadoqlash_performance['daily_pct']:.1f}%\n"
        report += f"      Ойлик фоиз: {qadoqlash_performance['monthly_pct']:.1f}%\n"
        report += f"      Қолган иш: {qadoqlash_performance['remaining_work']:.1f} та\n"
        report += f"      Ҳар кунги керак: {qadoqlash_performance['daily_needed']:.1f} та/кун\n\n"
        
        # Умумий
        total_today = bichish_ish + tasnif_total + tikuv_ish + qadoqlash_ish
        report += f"📈 Жами кунлик иш: {total_today} та\n"
        report += f"📆 Қолган иш кунлари: {qadoqlash_performance['remaining_days']} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_daily_report хato: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return "❌ Хисобот яратишда хатолик юз берди."

def format_weekly_report():
    try:
        weekly_rows = find_week_rows(sheet_report)
        if not weekly_rows:
            return "❌ Ҳафта учун маълумотлар мавжуд эмас."
            
        monthly_data = get_monthly_data()
        
        # Emoji lug'ati
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        # Ҳафталик маълумотларни йиғиш
        bichish_total = 0
        tasnif_total = 0
        tikuv_total = 0
        qadoqlash_total = 0
        
        for row_idx in weekly_rows:
            row = sheet_report.row_values(row_idx)
            bichish_total += safe_val(row, 1)
            tasnif_total += safe_val(row, 3) + safe_val(row, 4) + safe_val(row, 5)
            tikuv_total += safe_val(row, 7)
            qadoqlash_total += safe_val(row, 10)
        
        total_weekly = bichish_total + tasnif_total + tikuv_total + qadoqlash_total
        
        # Ҳафталик режаларни хисоблаш (ойлик режа / 4)
        bichish_weekly_plan = monthly_data.get('бичиш', {}).get('plan', 0) / 4
        tasnif_weekly_plan = monthly_data.get('тасниф', {}).get('plan', 0) / 4
        tikuv_weekly_plan = monthly_data.get('тикув', {}).get('plan', 0) / 4
        qadoqlash_weekly_plan = monthly_data.get('қадоқлаш', {}).get('plan', 0) / 4
        
        # Фоизларни хисоблаш
        bichish_weekly_pct = calculate_percentage(bichish_total, bichish_weekly_plan)
        tasnif_weekly_pct = calculate_percentage(tasnif_total, tasnif_weekly_plan)
        tikuv_weekly_pct = calculate_percentage(tikuv_total, tikuv_weekly_plan)
        qadoqlash_weekly_pct = calculate_percentage(qadoqlash_total, qadoqlash_weekly_plan)
        
        week_start, week_end = get_week_start_end_dates()
        week_number = get_week_number()
        
        report = f"📅 Ҳафталик хисобот ({week_number}-ҳафта, {week_start} - {week_end})\n\n"
        
        report += f"{section_emojis.get('бичиш', '✂️')} Бичиш: {bichish_total} та\n"
        report += f"   Ҳафталик режа: {bichish_weekly_plan:.0f} та | Бажарилди: {bichish_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, bichish_weekly_plan - bichish_total):.0f} та | Фоизда: {max(0, 100 - bichish_weekly_pct):.1f}%\n"
        
        remaining_days = get_remaining_workdays()
        daily_needed_bichish = (max(0, bichish_weekly_plan - bichish_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_bichish:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('тасниф', '📑')} Тасниф: {tasnif_total} та\n"
        report += f"   Ҳафталик режа: {tasnif_weekly_plan:.0f} та | Бажарилди: {tasnif_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, tasnif_weekly_plan - tasnif_total):.0f} та | Фоизда: {max(0, 100 - tasnif_weekly_pct):.1f}%\n"
        
        daily_needed_tasnif = (max(0, tasnif_weekly_plan - tasnif_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_tasnif:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('тикув', '🧵')} Тикув: {tikuv_total} та\n"
        report += f"   Ҳафталик режа: {tikuv_weekly_plan:.0f} та | Бажарилди: {tikuv_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, tikuv_weekly_plan - tikuv_total):.0f} та | Фоизда: {max(0, 100 - tikuv_weekly_pct):.1f}%\n"
        
        daily_needed_tikuv = (max(0, tikuv_weekly_plan - tikuv_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_tikuv:.1f} та/кун\n\n"
        
        report += f"{section_emojis.get('қадоқлаш', '📦')} Қадоқлаш: {qadoqlash_total} та\n"
        report += f"   Ҳафталик режа: {qadoqlash_weekly_plan:.0f} та | Бажарилди: {qadoqlash_weekly_pct:.1f}%\n"
        report += f"   Қолдиқ: {max(0, qadoqlash_weekly_plan - qadoqlash_total):.0f} та | Фоизда: {max(0, 100 - qadoqlash_weekly_pct):.1f}%\n"
        
        daily_needed_qadoqlash = (max(0, qadoqlash_weekly_plan - qadoqlash_total) / remaining_days) if remaining_days > 0 else 0
        report += f"   Ҳар кунги керак: {daily_needed_qadoqlash:.1f} та/кун\n\n"
        
        report += f"📊 Жами ҳафталик иш: {total_weekly} та\n"
        report += f"📆 Ҳафта охиригача қолган иш кунлари: {remaining_days} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_weekly_report хato: {e}")
        return "❌ Ҳафталик хисобот яратишда хатолик юз берди."

def format_monthly_report():
    try:
        monthly_data = get_monthly_data()
        if not monthly_data:
            return "❌ Ойлик маълумотлар мавжуд эмас."
            
        report = f"🗓 Ойлик хисобот ({get_month_name()})\n\n"
        
        remaining_days = get_remaining_workdays()
        current_workday = get_current_workday_index()
        
        # Emoji lug'ati
        section_emojis = {
            "бичиш": "✂️",
            "тасниф": "📑", 
            "тикув": "🧵",
            "қадоқлаш": "📦"
        }
        
        for section_name, data in monthly_data.items():
            section_display = section_name.capitalize()
            monthly_plan = data['plan']
            monthly_done = data['done']
            
            # Foizlarni qayta hisoblash
            done_percentage = calculate_percentage(monthly_done, monthly_plan)
            remaining_percentage = calculate_percentage(data['remaining'], monthly_plan)
            
            daily_plan = monthly_plan / WORKING_DAYS_IN_MONTH
            daily_needed = data['remaining'] / remaining_days if remaining_days > 0 else 0
            
            # Emoji va format
            emoji = section_emojis.get(section_name, "📊")
            report += f"{emoji} {section_display}:\n"
            report += f"   • Ойлик режа: {monthly_plan:.0f} та ({WORKING_DAYS_IN_MONTH} иш куни), Кунлик норма: {daily_plan:.1f} та/кун\n"
            report += f"   • Бажарилди: {monthly_done:.0f} та ({done_percentage:.1f}%)\n"
            report += f"   • Қолдиқ: {data['remaining']:.0f} та ({remaining_percentage:.1f}%)\n"
            
            if remaining_days > 0:
                report += f"   • Ҳар кунги керак: {daily_needed:.1f} та/кун (Қолган иш кунлари: {remaining_days} кун)\n\n"
            else:
                report += f"   • Ой якунланди\n\n"
        
        # 100% дан ортиқ бажарилган бўлимларни текшириш
        congratulations = []
        for section_name, data in monthly_data.items():
            done_pct = float(data['done_pct'].replace('%', ''))
            if done_pct >= 100:
                section_display = section_name.capitalize()
                extra = data['done'] - data['plan']
                congratulations.append(f"🎉 {section_display} бўлими ойлик режани {done_pct:.1f}% бажариб, режадан {extra:.0f} та ортиқ иш чиқарди!")
        
        if congratulations:
            report += "\n".join(congratulations) + "\n\n"
        
        report += f"📈 Жами иш кунлари: {WORKING_DAYS_IN_MONTH} кун\n"
        report += f"📅 Ҳозиргача иш кунлари: {current_workday} кун\n"
        report += f"📆 Қолган иш кунлари: {remaining_days} кун"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_monthly_report хato: {e}")
        return "❌ Ойлик хисобот яратишда хатолик юз берди."

def format_orders_report():
    try:
        orders = get_orders_data()
        if not orders:
            return "❌ Ҳали буюртмалар мавжуд эмас."
            
        report = "📋 Буюртмалар хисоботи\n\n"
        
        for order in orders:
            report += f"📦 {order['name']}\n"
            report += f"   Миқдор: {order['done']}/{order['total']} та\n"
            report += f"   Бажарилди: {order['done_percentage']}\n"
            report += f"   Қолдиқ: {order['remaining']} та ({order['remaining_percentage']})\n"
            report += f"   Муддат: {order['deadline']}\n"
            report += f"   Қолган кун: {order['days_left']}\n"
            report += f"   Бўлим: {order['section']}\n\n"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ format_orders_report хato: {e}")
        return "❌ Буюртмалар хисоботини яратишда хатолик юз берди."

# ------------------- ТУГМАЛАР -------------------
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Кунлик иш (бўлим бўйича)", callback_data="daily_work_by_section")],
        [InlineKeyboardButton(text="📦 Буюртмалар кунлик иши", callback_data="daily_work_by_order")],
        [InlineKeyboardButton(text="📋 Буюртмалар", callback_data="orders_menu")],
        [InlineKeyboardButton(text="📈 Хисоботлар", callback_data="reports_menu")],
        [InlineKeyboardButton(text="📊 График хисоботлар", callback_data="graph_reports")]
    ])

def daily_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="section_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="section_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="section_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="section_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def order_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="order_section_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="order_section_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="order_section_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="order_section_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def reports_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Кунлик хисобот", callback_data="kunlik")],
        [InlineKeyboardButton(text="📅 Хафталик хисобот", callback_data="haftalik")],
        [InlineKeyboardButton(text="🗓 Ойлик хисобот", callback_data="oylik")],
        [InlineKeyboardButton(text="📋 Буюртмалар хисоботи", callback_data="orders_report")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def graph_reports_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Ойлик иш графиги", callback_data="graph_monthly")],
        [InlineKeyboardButton(text="📈 Кунлик иш графиги", callback_data="graph_daily")],
        [InlineKeyboardButton(text="📋 Буюртмалар графиги", callback_data="graph_orders")],
        [InlineKeyboardButton(text="📉 Кунлик тенденция", callback_data="graph_trend")],
        [InlineKeyboardButton(text="📅 Ҳафталик тенденция", callback_data="graph_weekly_trend")],
        [InlineKeyboardButton(text="🗓 Ойлик тенденция", callback_data="graph_monthly_trend")],
        [InlineKeyboardButton(text="🥧 Фоизлар диаграммаси", callback_data="graph_percentage_pie")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def orders_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Буюртмалар рўйхати", callback_data="orders_list")],
        [InlineKeyboardButton(text="➕ Янги буюртма", callback_data="add_order")],
        [InlineKeyboardButton(text="✏️ Буюртмани таҳрирлаш", callback_data="edit_order")],
        [InlineKeyboardButton(text="✅ Кунлик иш қўшиш", callback_data="add_daily_work")],
        [InlineKeyboardButton(text="🗑 Буюртмани ўчириш", callback_data="delete_order")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def order_edit_menu(order_name, row_index):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Номини ўзгартириш", callback_data=f"edit_name:{order_name}:{row_index}")],
        [InlineKeyboardButton(text="📦 Миқдорни ўзгартириш", callback_data=f"edit_quantity:{order_name}:{row_index}")],
        [InlineKeyboardButton(text="✅ Бажарилганини ўзгартириш", callback_data=f"edit_done:{order_name}:{row_index}")],
        [InlineKeyboardButton(text="📅 Муддатни ўзгартириш", callback_data=f"edit_deadline:{order_name}:{row_index}")],
        [InlineKeyboardButton(text="🏷 Бўлимини ўзгартириш", callback_data=f"edit_section:{order_name}:{row_index}")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_orders")]
    ])

def sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="section_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="section_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="section_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="section_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_orders")]
    ])

def daily_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="section_select_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="section_select_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="section_select_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="section_select_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])

def orders_keyboard(orders):
    keyboard = []
    for order in orders:
        keyboard.append([InlineKeyboardButton(
            text=f"📦 {order['name']} ({order['done']}/{order['total']})", 
            callback_data=f"select_order:{order['name']}:{order['row_index']}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_orders")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def orders_keyboard_by_section(orders):
    buttons = []
    for order in orders:
        buttons.append([InlineKeyboardButton(
            text=f"📦 {order['name']} ({order['done']}/{order['total']})",
            callback_data=f"daily_section_order:{order['name']}:{order['row_index']}"
        )])
    
    # "Ортга" тугмасини қўшиш
    buttons.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_daily_sections")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

#------------------- START va SETTINGS -------------------

@dp.message(Command("start"))
async def start_cmd(message: Message):
    logger.info(f"🚀 /start командаси: {message.from_user.first_name} ({message.from_user.id})")
    
    # Faqat asosiy menyuni yuborish
    await message.answer("Ассалому алейкум! 👋\nБўлимни танланг:", reply_markup=main_menu())


@dp.message(Command("hisobot"))
async def hisobot_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    logger.info(f"📊 /hisobot командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Хисобот турini танланг:", reply_markup=reports_menu())

@dp.message(Command("buyurtmalar"))
async def buyurtmalar_cmd(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Сизда бу имконият йўқ.")
        return
        
    logger.info(f"📋 /buyurtmalar командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.message(Command("kunlik_ish"))
async def kunlik_ish_cmd(message: Message):
    logger.info(f"📝 /kunlik_ish командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())

@dp.message(Command("grafik"))
async def grafik_cmd(message: Message):
    logger.info(f"📈 /grafik командаси: {message.from_user.first_name} ({message.from_user.id})")
    await message.answer("График хисобот турini танланг:", reply_markup=graph_reports_menu())

@dp.message(F.web_app_data)
async def handle_web_app_data(message: Message, state: FSMContext):
    try:
        data = json.loads(message.web_app_data.data)
        action = data.get('action')
        
        if action == "daily_report":
            report = format_daily_report()
            await message.answer(report)
            
        elif action == "add_section_data":
            section = data.get('section')
            values_by_index = {}
            monthly_section = ""
            ish_soni = 0
            
            if section == "bichish":
                values_by_index = {
                    1: data.get('ish_soni', 0),
                    2: data.get('hodim_soni', 0)
                }
                monthly_section = "Бичиш"
                ish_soni = data.get('ish_soni', 0)
                
            elif section == "tasnif":
                values_by_index = {
                    3: data.get('dikimga', 0),
                    4: data.get('pechat', 0),
                    5: data.get('vishivka', 0),
                    6: data.get('hodim_soni', 0)
                }
                monthly_section = "Тасниф"
                ish_soni = data.get('dikimga', 0) + data.get('pechat', 0) + data.get('vishivka', 0)
                
            elif section == "tikuv":
                values_by_index = {
                    7: data.get('ish_soni', 0),
                    8: data.get('hodim_soni', 0),
                    9: data.get('oyoqchi_hodim', 0)
                }
                monthly_section = "Тикув"
                ish_soni = data.get('ish_soni', 0)
                
            elif section == "qadoqlash":
                values_by_index = {
                    10: data.get('ish_soni', 0),
                    11: data.get('hodim_soni', 0)
                }
                monthly_section = "Қадоқлаш"
                ish_soni = data.get('ish_soni', 0)
            
            if append_or_update(sheet_report, values_by_index):
                congrats_msg = update_monthly_totals(monthly_section, ish_soni)
                await message.answer(f"✅ {section} маълумотлари сақланди!")
                
                if congrats_msg:
                    await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
            else:
                await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
            
        elif action == "add_order":
            order_name = data.get('name')
            quantity = data.get('quantity')
            deadline = data.get('deadline')
            section = data.get('section')
            
            errors = validate_order_data(order_name, quantity, deadline)
            if errors:
                error_message = "\n".join(errors)
                await message.answer(error_message)
                return
            
            try:
                deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
                today = datetime.now(TZ)
                days_left = (deadline_date - today).days
                
                new_row = [
                    today_date_str(),
                    order_name,
                    quantity,
                    0,
                    quantity,
                    "0%",
                    "100%",
                    deadline,
                    days_left,
                    section
                ]
                
                sheet_orders.append_row(new_row)
                success_message = f"✅ Буюртма қўшилди:\n\nНоми: {order_name}\nМиқдори: {quantity}\nМуддати: {deadline}\nБўлим: {section}"
                await message.answer(success_message)
                
                group_message = f"📦 Янги буюртма:\n\nНоми: {order_name}\nМиқдори: {quantity} та\nМуддати: {deadline}\nБўлим: {section}\nҚолган кунлар: {days_left}"
                await send_to_group(group_message, ORDERS_TOPIC_ID)
                
            except Exception as e:
                await message.answer(f"❌ Хатолик юз берди: {e}")
            finally:
                await state.clear()
                await message.answer("Бош меню:", reply_markup=main_menu())
        elif action == "get_report":
            report_type = data.get('report_type')
            if report_type == "daily":
                report = format_daily_report()
            elif report_type == "weekly":
                report = format_weekly_report()
            elif report_type == "monthly":
                report = format_monthly_report()
            elif report_type == "orders":
                report = format_orders_report()
            else:
                report = "❌ Номаълум хисобот тури"
            
            await message.answer(report)       

            
        elif action == "refresh_data":
            # Dashboard ma'lumotlarini yuborish
            monthly_data = get_monthly_data()
            orders = get_orders_data()
            
            # Kunlik ish - bugungi kun uchun barcha bo'limlardagi ish miqdori
            today = today_date_str()
            today_work = 0
            
            try:
                # Bugungi kun uchun ma'lumotlarni olish
                records = sheet_report.get_all_values()
                for row in records:
                    if row and row[0] == today:
                        # Bichish
                        if len(row) > 1 and row[1]:
                            today_work += parse_int(row[1])
                        # Tasnif (dikimga + pechat + vishivka)
                        if len(row) > 3 and row[3]:
                            today_work += parse_int(row[3])
                        if len(row) > 4 and row[4]:
                            today_work += parse_int(row[4])
                        if len(row) > 5 and row[5]:
                            today_work += parse_int(row[5])
                        # Tikuv
                        if len(row) > 7 and row[7]:
                            today_work += parse_int(row[7])
                        # Qadoqlash
                        if len(row) > 10 and row[10]:
                            today_work += parse_int(row[10])
                        break
            except Exception as e:
                logger.error(f"Bugungi ish miqdorini hisoblashda xato: {e}")
            
            # Haftalik ish - oxirgi 7 kunlik ish miqdori
            weekly_work = 0
            try:
                # Oxirgi 7 kunlik ma'lumotlarni olish
                records = sheet_report.get_all_values()
                today_date = datetime.now(TZ)
                
                for i in range(1, min(8, len(records))):
                    if len(records[i]) > 0:
                        try:
                            row_date = datetime.strptime(records[i][0], "%d.%m.%Y").replace(tzinfo=TZ)
                            # Faqat oxirgi 7 kunlik ma'lumotlarni hisoblaymiz
                            if (today_date - row_date).days <= 7:
                                # Bichish
                                if len(records[i]) > 1 and records[i][1]:
                                    weekly_work += parse_int(records[i][1])
                                # Tasnif
                                if len(records[i]) > 3 and records[i][3]:
                                    weekly_work += parse_int(records[i][3])
                                if len(records[i]) > 4 and records[i][4]:
                                    weekly_work += parse_int(records[i][4])
                                if len(records[i]) > 5 and records[i][5]:
                                    weekly_work += parse_int(records[i][5])
                                # Tikuv
                                if len(records[i]) > 7 and records[i][7]:
                                    weekly_work += parse_int(records[i][7])
                                # Qadoqlash
                                if len(records[i]) > 10 and records[i][10]:
                                    weekly_work += parse_int(records[i][10])
                        except:
                            continue
            except Exception as e:
                logger.error(f"Haftalik ish miqdorini hisoblashda xato: {e}")
            
            # Oylik reja foizi
            monthly_plan_percentage = 0
            try:
                total_plan = 0
                total_done = 0
                for section, data in monthly_data.items():
                    total_plan += data['plan']
                    total_done += data['done']
                
                if total_plan > 0:
                    monthly_plan_percentage = round((total_done / total_plan) * 100, 1)
            except Exception as e:
                logger.error(f"Oylik reja foizini hisoblashda xato: {e}")
            
            # Jami buyurtmalar soni
            total_orders = len(orders)
            
            # JSON formatida javob qaytarish
            response = {
                'action': 'refresh_data_response',
                'daily_work': today_work,
                'weekly_work': weekly_work,
                'monthly_plan': monthly_plan_percentage,
                'total_orders': total_orders
            }
            
            await message.answer(json.dumps(response))
            
        elif action == "get_orders":
            orders = get_orders_data()
            
            # JSON formatida javob qaytarish
            response = {
                'action': 'get_orders_response',
                'orders': orders
            }
            
            await message.answer(json.dumps(response))
            
        elif action == "get_dashboard_data":
            # Dashboard ma'lumotlarini yuborish
            monthly_data = get_monthly_data()
            orders = get_orders_data()
            
            # Kunlik ish - bugungi kun uchun barcha bo'limlardagi ish miqdori
            today = today_date_str()
            today_work = 0
            
            try:
                # Bugungi kun uchun ma'lumotlarni olish
                records = sheet_report.get_all_values()
                for row in records:
                    if row and row[0] == today:
                        # Bichish
                        if len(row) > 1 and row[1]:
                            today_work += parse_int(row[1])
                        # Tasnif (dikimga + pechat + vishivka)
                        if len(row) > 3 and row[3]:
                            today_work += parse_int(row[3])
                        if len(row) > 4 and row[4]:
                            today_work += parse_int(row[4])
                        if len(row) > 5 and row[5]:
                            today_work += parse_int(row[5])
                        # Tikuv
                        if len(row) > 7 and row[7]:
                            today_work += parse_int(row[7])
                        # Qadoqlash
                        if len(row) > 10 and row[10]:
                            today_work += parse_int(row[10])
                        break
            except Exception as e:
                logger.error(f"Bugungi ish miqdorini hisoblashda xato: {e}")
            
            # Haftalik ish - oxirgi 7 kunlik ish miqdori
            weekly_work = 0
            try:
                # Oxirgi 7 kunlik ma'lumotlarni olish
                records = sheet_report.get_all_values()
                today_date = datetime.now(TZ)
                
                for i in range(1, min(8, len(records))):
                    if len(records[i]) > 0:
                        try:
                            row_date = datetime.strptime(records[i][0], "%d.%m.%Y").replace(tzinfo=TZ)
                            # Faqat oxirgi 7 kunlik ma'lumotlarni hisoblaymiz
                            if (today_date - row_date).days <= 7:
                                # Bichish
                                if len(records[i]) > 1 and records[i][1]:
                                    weekly_work += parse_int(records[i][1])
                                # Tasnif
                                if len(records[i]) > 3 and records[i][3]:
                                    weekly_work += parse_int(records[i][3])
                                if len(records[i]) > 4 and records[i][4]:
                                    weekly_work += parse_int(records[i][4])
                                if len(records[i]) > 5 and records[i][5]:
                                    weekly_work += parse_int(records[i][5])
                                # Tikuv
                                if len(records[i]) > 7 and records[i][7]:
                                    weekly_work += parse_int(records[i][7])
                                # Qadoqlash
                                if len(records[i]) > 10 and records[i][10]:
                                    weekly_work += parse_int(records[i][10])
                        except:
                            continue
            except Exception as e:
                logger.error(f"Haftalik ish miqdorini hisoblashda xato: {e}")
            
            # Oylik reja foizi
            monthly_plan_percentage = 0
            try:
                total_plan = 0
                total_done = 0
                for section, data in monthly_data.items():
                    total_plan += data['plan']
                    total_done += data['done']
                
                if total_plan > 0:
                    monthly_plan_percentage = round((total_done / total_plan) * 100, 1)
            except Exception as e:
                logger.error(f"Oylik reja foizini hisoblashda xato: {e}")
            
            # Jami buyurtmalar soni
            total_orders = len(orders)
            
            # JSON formatida javob qaytarish
            response = {
                'action': 'get_dashboard_data_response',
                'daily_work': today_work,
                'weekly_work': weekly_work,
                'monthly_plan': monthly_plan_percentage,
                'total_orders': total_orders
            }
            
            await message.answer(json.dumps(response))
            
        elif action == "get_chart_data":
            # Grafika ma'lumotlarini yuborish
            monthly_data = get_monthly_data()
            
            # Oylik progress grafigi uchun ma'lumotlar
            monthly_progress = {
                'labels': [],
                'plans': [],
                'actuals': []
            }
            
            for section_name, data in monthly_data.items():
                monthly_progress['labels'].append(section_name.capitalize())
                monthly_progress['plans'].append(data['plan'])
                monthly_progress['actuals'].append(data['done'])
            
            # Produktivlik grafigi uchun ma'lumotlar (oxirgi 7 kun)
            productivity = {
                'days': [],
                'bichish': [],
                'tasnif': [],
                'tikuv': [],
                'qadoqlash': []
            }
            
            try:
                # Oxirgi 7 kunlik ma'lumotlarni olish
                records = sheet_report.get_all_values()
                today_date = datetime.now(TZ)
                
                # Kunlar ro'yxatini tayyorlash
                for i in range(6, -1, -1):
                    date = today_date - timedelta(days=i)
                    productivity['days'].append(date.strftime("%d.%m"))
                
                # Har bir kun uchun ma'lumotlarni to'plash
                for day in productivity['days']:
                    # Dastlab barcha qiymatlarni 0 ga tenglaymiz
                    productivity['bichish'].append(0)
                    productivity['tasnif'].append(0)
                    productivity['tikuv'].append(0)
                    productivity['qadoqlash'].append(0)
                    
                    # Ma'lumotlarni qidirish
                    for row in records:
                        if row and row[0] == day:
                            # Bichish
                            if len(row) > 1 and row[1]:
                                productivity['bichish'][-1] = parse_int(row[1])
                            # Tasnif (dikimga + pechat + vishivka)
                            tasnif_total = 0
                            if len(row) > 3 and row[3]:
                                tasnif_total += parse_int(row[3])
                            if len(row) > 4 and row[4]:
                                tasnif_total += parse_int(row[4])
                            if len(row) > 5 and row[5]:
                                tasnif_total += parse_int(row[5])
                            productivity['tasnif'][-1] = tasnif_total
                            # Tikuv
                            if len(row) > 7 and row[7]:
                                productivity['tikuv'][-1] = parse_int(row[7])
                            # Qadoqlash
                            if len(row) > 10 and row[10]:
                                productivity['qadoqlash'][-1] = parse_int(row[10])
                            break
            except Exception as e:
                logger.error(f"Produktivlik ma'lumotlarini yig'ishda xato: {e}")
            
            # Bo'limlar taqsimoti uchun ma'lumotlar
            sections = {
                'labels': [],
                'values': []
            }
            
            for section_name, data in monthly_data.items():
                sections['labels'].append(section_name.capitalize())
                sections['values'].append(data['done'])
            
            # Haftalik trend uchun ma'lumotlar
            weekly_trend = {
                'days': productivity['days'],
                'values': []
            }
            
            # Har bir kun uchun jami ish miqdori
            for i in range(len(productivity['days'])):
                total = productivity['bichish'][i] + productivity['tasnif'][i] + productivity['tikuv'][i] + productivity['qadoqlash'][i]
                weekly_trend['values'].append(total)
            
            response = {
                'action': 'get_chart_data_response',
                'monthly_progress': monthly_progress,
                'productivity': productivity,
                'sections': sections,
                'weekly_trend': weekly_trend
            }
            
            await message.answer(json.dumps(response))
            
    except Exception as e:
        logger.error(f"Web App ma'lumotlarini qayta ishlashda xato: {e}")
        await message.answer("❌ Маълумотlarni qayta ishlashda xatolik yuz berdi")




# ------------------- БЎЛИМ HANDLERЛАРИ -------------------
sections_config = {
    "bichish": {"cols": [1, 2], "monthly_section": "Бичиш"},
    "tasnif": {"cols": [3, 4, 5, 6], "monthly_section": "Тасниф"},
    "tikuv": {"cols": [7, 8, 9], "monthly_section": "Тикув"},
    "qadoqlash": {"cols": [10, 11], "monthly_section": "Қадоқлаш"},
}

async def start_section(callback: CallbackQuery, state: FSMContext, section):
    await callback.answer()
    await state.update_data(section=section)
    logger.info(f"📝 {section} бўлими бошланди: {callback.from_user.first_name}")
    
    if section == "tikuv":
        await callback.message.answer("🧵 Тикув: Иш сонини киритинг:")
        await state.set_state(SectionStates.tikuv_ish)
    else:
        await callback.message.answer(f"{section.title()}: Иш сонини киритинг:")
        await state.set_state(SectionStates.ish_soni)

@dp.callback_query(F.data=="bichish")
async def cb_bichish(callback: CallbackQuery, state: FSMContext):
    await start_section(callback, state, "bichish")

@dp.callback_query(F.data=="tasnif")
async def cb_tasnif(callback: CallbackQuery, state: FSMContext):
    await start_section(callback, state, "tasnif")

@dp.callback_query(F.data=="tikuv")
async def cb_tikuv(callback: CallbackQuery, state: FSMContext):
    await start_section(callback, state, "tikuv")

@dp.callback_query(F.data=="qadoqlash")
async def cb_qadoqlash(callback: CallbackQuery, state: FSMContext):
    await start_section(callback, state, "qadoqlash")

@dp.callback_query(F.data=="reports_menu")
async def cb_reports_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Хисобот турini танланг:", reply_markup=reports_menu())

@dp.callback_query(F.data=="graph_reports")
async def cb_graph_reports(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("График хисобот турini танланг:", reply_markup=graph_reports_menu())

@dp.callback_query(F.data=="orders_menu")
async def cb_orders_menu(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.callback_query(F.data=="back_to_main")
async def cb_back_to_main(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Ассалому алейкум! 👋\nБўлимни танланг:", reply_markup=main_menu())

@dp.callback_query(F.data=="back_to_orders")
async def cb_back_to_orders(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("Буюртмалар бўлими:", reply_markup=orders_menu())

@dp.callback_query(F.data=="back_to_daily_sections")
async def cb_back_to_daily_sections(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())

@dp.callback_query(F.data=="orders_list")
async def cb_orders_list(callback: CallbackQuery):
    await callback.answer()
    orders_text = format_orders_report()
    await callback.message.answer(orders_text, reply_markup=orders_menu())

# ------------------- ГРАФИк ХИСОБОТЛАР -------------------
@dp.callback_query(F.data=="graph_monthly")
async def cb_graph_monthly(callback: CallbackQuery):
    await callback.answer()
    logger.info(f"📊 Ойлик график сўранди: {callback.from_user.first_name}")
    
    image_buf = create_monthly_trend_chart()
    if image_buf:
        # BytesIO дан bytes olish va BufferedInputFile ga o'tkazish
        photo = BufferedInputFile(
            file=image_buf.getvalue(), 
            filename='monthly_trend.png'
        )
        await callback.message.answer_photo(
            photo=photo, 
            caption="📊 Ойлик иш чиқимлари графиги"
        )
        image_buf.close()  # Buffer ni yopish
    else:
        await callback.message.answer("❌ Ойлик маълумотлари мавжуд эмас.")

@dp.callback_query(F.data=="graph_daily")
async def cb_graph_daily(callback: CallbackQuery):
    await callback.answer()
    logger.info(f"📈 Кунлик график сўранди: {callback.from_user.first_name}")
    
    image_buf = create_weekly_trend_chart()
    if image_buf:
        photo = BufferedInputFile(
            file=image_buf.getvalue(),
            filename='weekly_trend.png'
        )
        await callback.message.answer_photo(
            photo=photo, 
            caption="📈 Кунлик иш тақсимоти графиги"
        )
        image_buf.close()  # Buffer ni yopish
    else:
        await callback.message.answer("❌ Кунлик маълумотлари мавжуд эмас.")

@dp.callback_query(F.data=="graph_weekly_trend")
async def cb_graph_weekly_trend(callback: CallbackQuery):
    await callback.answer()
    logger.info(f"📅 Ҳафталик тенденция графиги сўранди: {callback.from_user.first_name}")
    
    image_buf = create_weekly_trend_chart()
    if image_buf:
        photo = BufferedInputFile(
            file=image_buf.getvalue(),
            filename='weekly_trend.png'
        )
        await callback.message.answer_photo(
            photo=photo, 
            caption="📅 Ҳафталик иш чиқими тенденцияси"
        )
        image_buf.close()
    else:
        await callback.message.answer("❌ Ҳафталик маълумотлари мавжуд эмас.")

@dp.callback_query(F.data=="graph_monthly_trend")
async def cb_graph_monthly_trend(callback: CallbackQuery):
    await callback.answer()
    logger.info(f"🗓 Ойлик тенденция графиги сўранди: {callback.from_user.first_name}")
    
    image_buf = create_monthly_trend_chart()
    if image_buf:
        photo = BufferedInputFile(
            file=image_buf.getvalue(),
            filename='monthly_trend.png'
        )
        await callback.message.answer_photo(
            photo=photo, 
            caption="🗓 Ойлик иш чиқими тенденцияси"
        )
        image_buf.close()
    else:
        await callback.message.answer("❌ Ойлик маълумотлари мавжуд эмас.")

@dp.callback_query(F.data=="graph_percentage_pie")
async def cb_graph_percentage_pie(callback: CallbackQuery):
    await callback.answer()
    logger.info(f"🥧 Фоизлар диаграммаси сўранди: {callback.from_user.first_name}")
    
    # Проверяем наличие данных
    monthly_data = get_monthly_data()
    if not monthly_data:
        await callback.message.answer("❌ Ойлик маълумотлар мавжуд эмас. Аввал маълумотларни киритинг.")
        return
    
    image_buf = create_percentage_pie_chart()
    if image_buf:
        photo = BufferedInputFile(
            file=image_buf.getvalue(),
            filename='percentage_pie.png'
        )
        await callback.message.answer_photo(
            photo=photo, 
            caption="🥧 Ойлик режа бажарилиши фоизда"
        )
        image_buf.close()
    else:
        await callback.message.answer("❌ Фоизлар учун маълумотлар мавжуд эмас ёки хатолик юз берди.")

# ------------------- ХИСОБОТ HANDLERЛАРИ -------------------
@dp.callback_query(F.data=="kunlik")
async def cb_kunlik(callback: CallbackQuery):
    await callback.answer()
    report = format_daily_report()
    await callback.message.answer(report)

@dp.callback_query(F.data=="haftalik")
async def cb_haftalik(callback: CallbackQuery):
    await callback.answer()
    report = format_weekly_report()
    await callback.message.answer(report)

@dp.callback_query(F.data=="oylik")
async def cb_oylik(callback: CallbackQuery):
    await callback.answer()
    report = format_monthly_report()
    await callback.message.answer(report)

@dp.callback_query(F.data=="orders_report")
async def cb_orders_report(callback: CallbackQuery):
    await callback.answer()
    report = format_orders_report()
    await callback.message.answer(report)

# ------------------- КУНЛИК ИШ (БЎЛИМ БЎЙИЧА) -------------------
# Бўлимлар рўйхати учун клавиатура
def daily_sections_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✂️ Бичиш", callback_data="section_select_bichish")],
        [InlineKeyboardButton(text="📑 Тасниф", callback_data="section_select_tasnif")],
        [InlineKeyboardButton(text="🧵 Тикув", callback_data="section_select_tikuv")],
        [InlineKeyboardButton(text="📦 Қадоқлаш", callback_data="section_select_qadoqlash")],
        [InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_main")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data == "daily_work_by_section")
async def cb_daily_work_by_section(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Кунлик иш қўшиш учун бўлимни танланг:", reply_markup=daily_sections_keyboard())
    await state.set_state(DailyWorkStates.waiting_for_section)

@dp.callback_query(F.data == "daily_work_by_order")
async def cb_daily_work_by_order(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("Буюртма бўлимини танланг:", reply_markup=order_sections_keyboard())
    await state.set_state(DailyWorkStates.waiting_for_section)

@dp.callback_query(F.data.startswith("order_section_"))
async def cb_order_section_select(callback: CallbackQuery, state: FSMContext):
    section_key = callback.data.replace("order_section_", "")
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    if section_key not in section_names:
        await callback.answer("❌ Нотўғри бўлим")
        return
        
    section_name = section_names[section_key]
    orders = get_orders_by_section(section_name)
    
    if not orders:
        await callback.message.answer(f"❌ {section_name} бўлимида ҳозирча актив буюртмалар мавжуд эмас.")
        await state.clear()
        return
        
    await state.update_data(section=section_name)
    await callback.message.answer(f"📦 {section_name} бўлимидаги буюртмани танланг:", reply_markup=orders_keyboard_by_section(orders))
    await state.set_state(DailyWorkStates.waiting_for_order)

@dp.callback_query(F.data.startswith("order_section_"))
async def cb_order_section_select(callback: CallbackQuery, state: FSMContext):
    section_key = callback.data.replace("order_section_", "")
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    if section_key not in section_names:
        await callback.answer("❌ Нотўғри бўлим")
        return
        
    section_name = section_names[section_key]
    orders = get_orders_by_section(section_name)
    
    if not orders:
        await callback.message.answer(f"❌ {section_name} бўлимида ҳозирча актив буюртмалар мавжуд эмас.")
        await state.clear()
        return
        
    await state.update_data(section=section_name)
    await callback.message.answer(f"📦 {section_name} бўлимидаги буюртмани танланг:", reply_markup=orders_keyboard_by_section(orders))
    await state.set_state(DailyWorkStates.waiting_for_order)
    
    # Debug uchun consolega chiqarish
    logger.info(f"Section key received: {section_key}")
    logger.info(f"Available sections: {list(section_names.keys())}")
    
    if section_key not in section_names:
        await callback.answer("❌ Noto'g'ri bo'lim")
        return
        
    section_name = section_names[section_key]
    orders = get_orders_by_section(section_name)
    
    if not orders:
        await callback.message.answer(f"❌ {section_name} бўлимида ҳозирча актив буюртмалар мавжуд эмас.")
        await state.clear()
        return
        
    await state.update_data(section=section_name, section_key=section_key)
    await callback.message.answer(f"📦 {section_name} бўлимидаги буюртмани танланг:", reply_markup=orders_keyboard_by_section(orders))
    await state.set_state(DailyWorkStates.waiting_for_order)

@dp.callback_query(F.data.in_(["daily_section_bichish", "daily_section_tasnif", "daily_section_tikuv", "daily_section_qadoqlash"]))
async def cb_daily_section_select(callback: CallbackQuery, state: FSMContext):
    section_key = callback.data.replace("daily_section_", "")
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    # Debug uchun consolega chiqarish
    logger.info(f"Section key received: {section_key}")
    logger.info(f"Available sections: {list(section_names.keys())}")
    
    if section_key not in section_names:
        await callback.answer("❌ Noto'g'ri bo'lim")
        return
        
    section_name = section_names[section_key]
    orders = get_orders_by_section(section_name)
    
    if not orders:
        await callback.message.answer(f"❌ {section_name} бўлимида ҳозирча актив буюртмалар мавжуд эмас.")
        await state.clear()
        return
        
    await state.update_data(section=section_name, section_key=section_key)
    await callback.message.answer(f"📦 {section_name} бўлимидаги буюртмани танланг:", reply_markup=orders_keyboard_by_section(orders))
    await state.set_state(DailyWorkStates.waiting_for_order)

# Буюртмалар рўйхати учун клавиатура
def orders_keyboard_by_section(orders):
    buttons = []
    for order in orders:
        buttons.append([InlineKeyboardButton(
            text=f"📦 {order['name']} ({order['done']}/{order['total']})",
            callback_data=f"order_select:{order['name']}:{order['row_index']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_daily_sections")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def check_section_names_in_sheet():
    """Google Sheets-даги section номларини текшириш"""
    try:
        orders = get_orders_data()
        section_names = set()
        
        for order in orders:
            section = order.get('section', '')
            if section:
                section_names.add(section)
        
        logger.info(f"Google Sheets-даги section номлари: {section_names}")
        return section_names
    except Exception as e:
        logger.error(f"Section номларини текширишда хатолик: {e}")
        return set()

# Функцияни ишга тушириш
check_section_names_in_sheet()

# Буюртмани танлаш
@dp.callback_query(F.data.startswith("daily_section_order:"))
async def cb_daily_section_order(callback: CallbackQuery, state: FSMContext):
    data_parts = callback.data.split(":")
    if len(data_parts) < 3:
        await callback.answer("❌ Noto'g'ri format")
        return
        
    order_name = data_parts[1]
    row_index = int(data_parts[2])
    
    await callback.answer()
    await state.update_data(order_name=order_name, order_row=row_index)
    await callback.message.answer(f"📦 '{order_name}' буюртмаси учун кунлик иш миқдорини киритиng:")
    await state.set_state(DailyWorkStates.waiting_for_quantity)

# Миқдорни қабул қилиш
@dp.message(DailyWorkStates.waiting_for_quantity)
async def process_daily_work_quantity(message: Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❗️ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")
        return
        
    data = await state.get_data()
    section = data.get('section')
    row_index = data.get('order_row')
    section = data.get('section')
    
    # Buyurtma ma'lumotlarini olish
    order_data = sheet_orders.row_values(row_index)
    current_done = parse_float(order_data[3]) if len(order_data) > 3 else 0
    total = parse_float(order_data[2]) if len(order_data) > 2 else 0
    
    new_done = current_done + quantity
    if new_done > total:
        await message.answer(f"❌ Хатолик: Киргизилган миқдор жами микдордан ({total}) ошди. Қайта киритинг:")
        return
        
    if update_order_in_sheet(row_index, "done", new_done):
        # Bo'limning oylik hisobotini yangilash
        congrats_msg = update_monthly_totals(section, quantity)
        
        await message.answer(f"✅ '{order_name}' буюртмаси учун {quantity} та иш қўшилди. Жами: {new_done}/{total} та")
        
        if congrats_msg:
            await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
    else:
        await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
        
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

def normalize_section_name(section_name):
    section_mapping = {
        "bichish": "Бичиш",
        "бичиш": "Бичиш",
        "bichuv": "Бичиш",
        "cutting": "Бичиш",
        "tasnif": "Тасниф",
        "тасниф": "Тасниф", 
        "classify": "Тасниф",
        "tikuv": "Тикув",
        "тикув": "Тикув",
        "sewing": "Тикув",
        "qadoqlash": "Қадоқлаш",
        "қадоқлаш": "Қадоқлаш",
        "packing": "Қадоқлаш"
    }
    
    if not section_name:
        return ""
    
    normalized = section_name.strip().lower()
    return section_mapping.get(normalized, section_name)

def get_orders_by_section(section_name):
    orders = get_orders_data()
    normalized_section = normalize_section_name(section_name).strip().lower()
    
    result = []
    for order in orders:
        order_section = normalize_section_name(order.get('section', '')).strip().lower()
        if order_section == normalized_section and order['remaining'] > 0:
            result.append(order)
    
    return result

def get_orders_by_section(section_name):
    orders = get_orders_data()
    normalized_section = normalize_section_name(section_name).strip().lower()
    
    result = []
    for order in orders:
        order_section = normalize_section_name(order.get('section', '')).strip().lower()
        if order_section == normalized_section and order['remaining'] > 0:
            result.append(order)
    
    return result

@dp.callback_query(F.data.startswith("order_select:"))
async def cb_order_select(callback: CallbackQuery, state: FSMContext):
    data_parts = callback.data.split(":")
    if len(data_parts) < 3:
        await callback.answer("❌ Noto'g'ri format")
        return
        
    order_name = data_parts[1]
    row_index = int(data_parts[2])
    
    await callback.answer()
    await state.update_data(order_name=order_name, order_row=row_index)
    await callback.message.answer(f"📦 '{order_name}' буюртмаси учун кунлик иш миқдорини киритинг:")
    await state.set_state(DailyWorkStates.waiting_for_quantity)

# ------------------- БУЮРтмалаР -------------------
@dp.callback_query(F.data=="add_order")
async def cb_add_order(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.answer("📋 Янги буюртма номини киритинг:")
    await state.set_state(OrderStates.waiting_for_name)

@dp.message(OrderStates.waiting_for_name)
async def process_order_name(message: Message, state: FSMContext):
    order_name = message.text.strip()
    if len(order_name) < 2:
        await message.answer("❌ Буюртма номи энг камда 2 та ҳарфдан иборат бўлиши керак. Қайта киритинг:")
        return
        
    await state.update_data(order_name=order_name)
    await message.answer("📦 Буюртма микдорини киритинг:")
    await state.set_state(OrderStates.waiting_for_quantity)

@dp.message(OrderStates.waiting_for_quantity)
async def process_order_quantity(message: Message, state: FSMContext):
    try:
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❗️ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
            return
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")
        return
        
    await state.update_data(order_quantity=quantity)
    await message.answer("📅 Буюртма санасини киритинг (кун.ой.йил):")
    await state.set_state(OrderStates.waiting_for_date)

@dp.message(OrderStates.waiting_for_date)
async def process_order_date(message: Message, state: FSMContext):
    order_date = message.text.strip()
    try:
        datetime.strptime(order_date, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Санани нотоғри форматда киритдингиз. Қайта киритинг (кун.ой.йил):")
        return

    await state.update_data(order_date=order_date)
    await message.answer("📅 Буюртма муддатини киритинг (кун.ой.йил):")
    await state.set_state(OrderStates.waiting_for_deadline)

@dp.message(OrderStates.waiting_for_deadline)
async def process_order_deadline(message: Message, state: FSMContext):
    deadline = message.text.strip()
    
    data = await state.get_data()
    order_name = data.get('order_name')
    order_quantity = data.get('order_quantity')
    order_date = data.get('order_date', today_date_str())
    
    # Маълумотларни тексhirish
    errors = validate_order_data(order_name, order_quantity, deadline)
    if errors:
        error_message = "\n".join(errors)
        await message.answer(error_message)
        return

    try:  # ✅ Correct indentation
        deadline_date = datetime.strptime(deadline, "%d.%m.%Y").replace(tzinfo=TZ)
        today = datetime.now(TZ)
        days_left = (deadline_date - today).days
        
        new_row = [
            today_date_str(),
            order_name,
            order_quantity,
            0,
            order_quantity,
            "0%",
            "100%",
            deadline,
            days_left,
            section
        ]
        
        sheet_orders.append_row(new_row)
        success_message = f"✅ Буюртма қўшилди:\n\nНоми: {order_name}\nМиқдори: {order_quantity}\nМуддати: {deadline}\nБўлим: {section}"
        await message.answer(success_message)
        
        group_message = f"📦 Янги буюртма:\n\nНоми: {order_name}\nМиқдори: {order_quantity} та\nМуддати: {deadline}\nБўлим: {section}\nҚолган кунлар: {days_left}"
        await send_to_group(group_message, ORDERS_TOPIC_ID)
        
    except Exception as e:
        await message.answer(f"❌ Хатолик юз берди: {e}")
    finally:
        await state.clear()
        await message.answer("Бош меню:", reply_markup=main_menu())
@dp.callback_query(F.data.startswith("section_"))
async def cb_section_select(callback: CallbackQuery, state: FSMContext):
    section_key = callback.data.replace("section_", "")
    
    section_names = {
        "bichish": "Бичиш",
        "tasnif": "Тасниф", 
        "tikuv": "Тикув",
        "qadoqlash": "Қадоқлаш"
    }
    
    if section_key not in section_names:
        await callback.answer("❌ Нотўғри бўлим")
        return
        
    section_name = section_names[section_key]
    
    # Statega бўлим номини сақлаймиз
    await state.update_data(section=section_name)
    
    # Бўлимга қараб турли хабарлар
    if section_name == "Тасниф":
        await callback.message.answer(f"📑 {section_name} бўлими учун иш миқдорини киритинг (диқимга, печать, вишивка):\n\nМисол: 1000 200 50")
    else:
        await callback.message.answer(f"✳️ {section_name} бўлими учун кунлик иш миқдорини киритинг:")
    
    await state.set_state(DailyWorkStates.waiting_for_section_quantity)

@dp.message(DailyWorkStates.waiting_for_section_quantity)
async def process_section_quantity(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        section = data.get('section')
        
        if section == "Тасниф":
            # Тасниф учун алohда обработка (3та қиймат)
            parts = message.text.split()
            if len(parts) != 3:
                await message.answer("❌ Тасниф учун 3 та қиймат киритиш керак: диқимга, печать, вишивка\n\nМисол: 1000 200 50")
                return
                
            dikimga = int(parts[0])
            pechat = int(parts[1])
            vishivka = int(parts[2])
            
            if any(x < 0 for x in [dikimga, pechat, vishivka]):
                await message.answer("❗️ Барча миқдорлар мусбат сон бўлиши керак. Қайта киритинг:")
                return
                
            # Ходим сонини сўраймиз
            await state.update_data(dikimga=dikimga, pechat=pechat, vishivka=vishivka)
            await message.answer("👥 Тасниф ходим сонини киритинг:")
            await state.set_state(DailyWorkStates.waiting_for_section_workers)
            
        else:
            # Бошқа бўлимлар учун
            quantity = int(message.text)
            if quantity <= 0:
                await message.answer("❗️ Миқдор мусбат сон бўлиши керак. Қайта киритинг:")
                return
                
            await state.update_data(quantity=quantity)
            
            # Ходим сонини сўраймиз
            if section == "Бичиш":
                await message.answer("👥 Бичиш ходим сонини киритинг:")
            elif section == "Тикув":
                await message.answer("👥 Тикув ходим сонини киритинг:")
            elif section == "Қадоқлаш":
                await message.answer("👥 Қадоқлаш ходим сонини киритинг:")
                
            await state.set_state(DailyWorkStates.waiting_for_section_workers)
            
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")

# Ходим сонини қабул қилиш
@dp.message(DailyWorkStates.waiting_for_section_workers)
async def process_section_workers(message: Message, state: FSMContext):
    try:
        workers = int(message.text)
        if workers <= 0:
            await message.answer("❗️ Ходим сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        data = await state.get_data()
        section = data.get('section')
        
        # Google Sheets га сақлаш
        section_config = {
            "Бичиш": {"cols": [1, 2], "monthly_section": "Бичиш"},
            "Тасниф": {"cols": [3, 4, 5, 6], "monthly_section": "Тасниф"},
            "Тикув": {"cols": [7, 8, 9], "monthly_section": "Тикув"},
            "Қадоқлаш": {"cols": [10, 11], "monthly_section": "Қадоқлаш"}
        }
        
        if section not in section_config:
            await message.answer("❌ Бўлим номи нотўғри")
            await state.clear()
            return
            
        config = section_config[section]
        
        if section == "Тасниф":
            dikimga = data.get('dikimga', 0)
            pechat = data.get('pechat', 0)
            vishivka = data.get('vishivka', 0)
            total_work = dikimga + pechat + vishivka
            
            values_by_index = {
                config['cols'][0]: dikimga,   # Диқимга
                config['cols'][1]: pechat,    # Печать
                config['cols'][2]: vishivka,  # Вишивка
                config['cols'][3]: workers    # Ходим сони
            }
        else:
            quantity = data.get('quantity', 0)
            values_by_index = {
                config['cols'][0]: quantity,  # Иш миқдори
                config['cols'][1]: workers    # Ходим сони
            }
            total_work = quantity
        
        if append_or_update(sheet_report, values_by_index):
            congrats_msg = update_monthly_totals(config['monthly_section'], total_work)
            
            if section == "Тасниф":
                await message.answer(
                    f"✅ {section} бўлими учун иш қўшилди:\n"
                    f"Диқимга: {dikimga} та\n"
                    f"Печать: {pechat} та\n"
                    f"Вишивка: {vishivka} та\n"
                    f"Ходим: {workers} та\n"
                    f"Жами: {total_work} та"
                )
            else:
                await message.answer(
                    f"✅ {section} бўлими учун {quantity} та иш қўшилди. "
                    f"Ходим: {workers} та"
                )
            
            if congrats_msg:
                await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
        else:
            await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
        
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")
        return
        
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

                group_message = f"📦 Янги буюртма:\n\nНоми: {order_name}\nМиқдори: {quantity} та\nСанан: {today_date_str()}\nМуддати: {deadline}\nБўлим: {section}\nҚолган кунлар: {days_left}"
                await send_to_group(group_message, ORDERS_TOPIC_ID)
            except Exception as e:
                await message.answer(f"❌ Хатолик юз берди: {e}")        
            await state.clear()
            await callback.message.answer("Бош меню:", reply_markup=main_menu())

def orders_keyboard_by_section(orders):
    buttons = []
    for order in orders:
        buttons.append([InlineKeyboardButton(
            text=f"📦 {order['name']} ({order['done']}/{order['total']})",
            callback_data=f"daily_order_select:{order['name']}:{order['row_index']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Ортга", callback_data="back_to_daily_sections")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data.startswith("daily_order_select:"))
async def cb_daily_order_select(callback: CallbackQuery, state: FSMContext):
    data_parts = callback.data.split(":")
    if len(data_parts) < 3:
        await callback.answer("❌ Noto'g'ri format")
        return
        
    order_name = data_parts[1]
    row_index = int(data_parts[2])
    
    await callback.answer()
    await state.update_data(order_name=order_name, order_row=row_index)
    await callback.message.answer(f"📦 '{order_name}' буюртмаси учун кунлик иш миқдорини киритинг:")
    await state.set_state(DailyWorkStates.waiting_for_quantity)

# ------------------- БУЮРТМАЛАРНИ ТАҲРИРЛАШ -------------------
@dp.callback_query(F.data=="edit_order")
async def cb_edit_order(callback: CallbackQuery):
    await callback.answer()
    
    orders = get_orders_data()
    if not orders:
        await callback.message.answer("📋 Ҳозирча ҳеч қандай буюртма мавжуд эмас.")
        return
    
    await callback.message.answer("✏️ Таҳрирлаш учун буюртмани танланг:", reply_markup=orders_keyboard(orders))

@dp.callback_query(F.data.startswith("select_order:"))
async def cb_select_order(callback: CallbackQuery):
    data_parts = callback.data.split(":")
    order_name = data_parts[1]
    row_index = int(data_parts[2])
    
    await callback.answer()
    await callback.message.answer(
        f"✏️ '{order_name}' буюртмасини таҳрирлаш учун қандай ўзгартириш киритишни танланг:",
        reply_markup=order_edit_menu(order_name, row_index)
    )

@dp.callback_query(F.data.startswith("edit_done:"))
async def cb_edit_done(callback: CallbackQuery, state: FSMContext):
    data_parts = callback.data.split(":")
    order_name = data_parts[1]
    row_index = int(data_parts[2])
    
    await callback.answer()
    await state.update_data(edit_row_index=row_index, edit_order_name=order_name)
    await callback.message.answer(f"✅ '{order_name}' буюртмаси учун бажарилган миқдорни киритинг:")
    await state.set_state(OrderStates.edit_order_done)

@dp.message(OrderStates.edit_order_done)
async def process_edit_done(message: Message, state: FSMContext):
    try:
        new_done = int(message.text)
        if new_done < 0:
            await message.answer("❗️ Миқдор манфий бўлмаслиги керак. Қайта киритинг:")
            return
    except ValueError:
        await message.answer("❗️ Миқдорни нотоғри киритдингиз. Қайта киритинг:")
        return
        
    data = await state.get_data()
    row_index = data.get('edit_row_index')
    order_name = data.get('edit_order_name')
    
    if update_order_in_sheet(row_index, "done", new_done):
        # Бўлимни аниқлаш ва ойлик хисоботни янгилаш
        order_data = sheet_orders.row_values(row_index)
        section = order_data[9] if len(order_data) > 9 else ""
        total = parse_float(order_data[2]) if len(order_data) > 2 else 0
        
        if section:
            # Янги бажарилган миқдорни хисоблаш
            old_done = parse_float(order_data[3]) if len(order_data) > 3 else 0
            difference = new_done - old_done
            
            if difference != 0:
                congrats_msg = update_monthly_totals(section, difference)
                
                if congrats_msg:
                    await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
        
        await message.answer(f"✅ '{order_name}' буюртмаси учун бажарилган миқдор {new_done} тага ўзгартирилди.")
        
        group_message = f"✅ Буюртма бажарилгани ўзгартирилди:\n\nНом: {order_name}\nБажарилган миқдор: {new_done} та\nЖами: {total} та"
        await send_to_group(group_message, ORDERS_TOPIC_ID)
    else:
        await message.answer(f"❌ Буюртманинг бажарилган миқдорини ўзгартиришда хатолик юз берди.")
    
    await state.clear()
    await message.answer("Буюртмалар бўлими:", reply_markup=orders_menu())

# ------------------- FSM HANDLERLARI -------------------
@dp.message(SectionStates.ish_soni)
async def process_ish_soni(message: Message, state: FSMContext):
    try:
        ish_soni = int(message.text)
        if ish_soni <= 0:
            await message.answer("❗️ Иш сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        await state.update_data(ish_soni=ish_soni)
        data = await state.get_data()
        section = data.get('section')
        
        if section == "tasnif":
            await message.answer("📑 Дикимга қилинган иш сонини киритинг:")
            await state.set_state(SectionStates.dikimga)
        else:
            await message.answer(f"👥 {section.title()} ходим сонини киритинг:")
            await state.set_state(SectionStates.hodim_soni)
            
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.hodim_soni)
async def process_hodim_soni(message: Message, state: FSMContext):
    try:
        hodim_soni = int(message.text)
        if hodim_soni <= 0:
            await message.answer("❗️ Ходим сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        data = await state.get_data()
        section = data.get('section')
        ish_soni = data.get('ish_soni')
        
        # Маълумотларни Google Sheetsга сақлаш
        section_config = sections_config.get(section, {})
        cols = section_config.get("cols", [])
        monthly_section = section_config.get("monthly_section", "")
        
        if not cols:
            await message.answer("❌ Бўлим конфигурациясида хатолик.")
            await state.clear()
            return
            
        values_by_index = {cols[0]: ish_soni, cols[1]: hodim_soni}
        
        if append_or_update(sheet_report, values_by_index):
            # Ойлик хисоботни янгилаш
            congrats_msg = update_monthly_totals(monthly_section, ish_soni)
            
            # Хабар
            await message.answer(f"✅ {section.title()} маълумотлари сақланди:\nИш: {ish_soni} та\nХодим: {hodim_soni} та")
            
            # Гуруҳга хабар
            production_msg = f"✅ {section.title()} бўлимида иш чиқимлари:\n\nИш: {ish_soni} та\nХодим: {hodim_soni} та"
            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
            
            if congrats_msg:
                await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
        else:
            await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
            
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")
        return
        
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

# Тасниф учун алохида handlerлар
# ------------------- FSM HANDLERLARI -------------------
@dp.message(SectionStates.dikimga)
async def process_dikimga(message: Message, state: FSMContext):
    try:
        dikimga = int(message.text)
        if dikimga < 0:
            await message.answer("❗️ Дикимга қилинган иш сони манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(dikimga=dikimga)
        await message.answer("🖨 Печат қилинган иш сонини киритинг:")
        await state.set_state(SectionStates.pechat)
        
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.pechat)
async def process_pechat(message: Message, state: FSMContext):
    try:
        pechat = int(message.text)
        if pechat < 0:  # 0 kiritishga ruxsat beramiz
            await message.answer("❗️ Печат қилинган иш сони манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(pechat=pechat)
        await message.answer("🧵 Вишивка қилинган иш сонини киритинг:")
        await state.set_state(SectionStates.vishivka)
        
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.vishivka)
async def process_vishivka(message: Message, state: FSMContext):
    try:
        vishivka = int(message.text)
        if vishivka < 0:  # 0 kiritishga ruxsat beramiz
            await message.answer("❗️ Вишивка қилинган иш сони манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        await state.update_data(vishivka=vishivka)
        await message.answer("👥 Тасниф ходим сонини киритинг:")
        await state.set_state(SectionStates.hodim_soni)
        
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.hodim_soni)
async def process_hodim_soni(message: Message, state: FSMContext):
    try:
        hodim_soni = int(message.text)
        if hodim_soni <= 0:  # Ходим сони ҳалиҳам мусбат бўлиши керак
            await message.answer("❗️ Ходим сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        data = await state.get_data()
        section = data.get('section')
        
        if section == "tasnif":
            dikimga = data.get('dikimga', 0)
            pechat = data.get('pechat', 0)
            vishivka = data.get('vishivka', 0)
            
            # Тасниф учун жами иш сони
            ish_soni = dikimga + pechat + vishivka
            
            # Маълумотларни Google Sheetsга сақлаш
            values_by_index = {
                3: dikimga,  # Дикимга
                4: pechat,   # Печат
                5: vishivka, # Вишивка
                6: hodim_soni # Ходим сони
            }
            
            if append_or_update(sheet_report, values_by_index):
                # Ойлик хисоботни янгилаш
                congrats_msg = update_monthly_totals("Тасниф", ish_soni)
                
                # Хабар
                await message.answer(f"✅ Тасниф маълумотлари сақланди:\nДикимга: {dikimga} та\nПечат: {pechat} та\nВишивка: {vishivka} та\nХодим: {hodim_soni} та\nЖами: {ish_soni} та")
                
                # Гуруҳга хабар
                production_msg = f"✅ Тасниф бўлимида иш чиқимлари:\n\nДикимга: {dikimga} та\nПечат: {pechat} та\nВишивка: {vishivka} та\nХодим: {hodim_soni} та\nЖами: {ish_soni} та"
                await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
                
                if congrats_msg:
                    await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
            else:
                await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
        else:
            # Бошқа бўлимлар учун аввалги логика
            ish_soni = data.get('ish_soni')
            
            # Маълумотларни Google Sheetsга сақлаш
            section_config = sections_config.get(section, {})
            cols = section_config.get("cols", [])
            monthly_section = section_config.get("monthly_section", "")
            
            if not cols:
                await message.answer("❌ Бўлим конфигурациясида хатолик.")
                await state.clear()
                return
                
            values_by_index = {cols[0]: ish_soni, cols[1]: hodim_soni}
            
            if append_or_update(sheet_report, values_by_index):
                # Ойлик хисоботни янгилаш
                congrats_msg = update_monthly_totals(monthly_section, ish_soni)
                
                # Хабар
                await message.answer(f"✅ {section.title()} маълумотлари сақланди:\nИш: {ish_soni} та\nХодим: {hodim_soni} та")
                
                # Гуруҳга хабар
                production_msg = f"✅ {section.title()} бўлимида иш чиқимлари:\n\nИш: {ish_soni} та\nХодим: {hodim_soni} та"
                await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
                
                if congrats_msg:
                    await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
            else:
                await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
            
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")
        return
        
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

@dp.message(SectionStates.tikuv_ish)
async def process_tikuv_ish(message: Message, state: FSMContext):
    try:
        tikuv_ish = int(message.text)
        if tikuv_ish <= 0:
            await message.answer("❗️ Иш сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        await state.update_data(tikuv_ish=tikuv_ish)
        await message.answer("👥 Тикув ходим сонини киритинг:")
        await state.set_state(SectionStates.tikuv_hodim)
        
    except ValueError:
        await message.answer("❗️ Иш сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.tikuv_hodim)
async def process_tikuv_hodim(message: Message, state: FSMContext):
    try:
        tikuv_hodim = int(message.text)
        if tikuv_hodim <= 0:
            await message.answer("❗️ Тикув ходим сони мусбат сон бўлиши керак. Қайта киритинг:")
            return
            
        await state.update_data(tikuv_hodim=tikuv_hodim)
        await message.answer("👞 Оёқчи ходим сонини киритинг:")
        await state.set_state(SectionStates.oyoqchi_hodim)
        
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")

@dp.message(SectionStates.oyoqchi_hodim)
async def process_oyoqchi_hodim(message: Message, state: FSMContext):
    try:
        oyoqchi_hodim = int(message.text)
        if oyoqchi_hodim < 0:
            await message.answer("❗️ Оёқчи ходим сони манфий бўлмаслиги керак. Қайта киритинг:")
            return
            
        data = await state.get_data()
        tikuv_ish = data.get('tikuv_ish')
        tikuv_hodim = data.get('tikuv_hodim')
        
        # Маълумотларни Google Sheetsга сақлаш
        values_by_index = {7: tikuv_ish, 8: tikuv_hodim, 9: oyoqchi_hodim}
        
        if append_or_update(sheet_report, values_by_index):
            # Ойлик хисоботни янгилаш
            congrats_msg = update_monthly_totals("Тикув", tikuv_ish)
            
            # Хабар
            await message.answer(f"✅ Тикув маълумотлари сақланди:\nИш: {tikuv_ish} та\nХодим: {tikuv_hodim} та\nОёқчи: {oyoqchi_hodim} та")
            
            # Гуруҳга хабар
            production_msg = f"✅ Тикув бўлимида иш чиқимлари:\n\nИш: {tikuv_ish} та\nХодим: {tikuv_hodim} та\nОёқчи: {oyoqchi_hodim} та"
            await send_to_group(production_msg, PRODUCTION_TOPIC_ID)
            
            if congrats_msg:
                await send_to_group(congrats_msg, RECOGNITION_TOPIC_ID)
        else:
            await message.answer("❌ Маълумотларни сақлашда хатолик юз берди.")
            
    except ValueError:
        await message.answer("❗️ Ходим сонини нотоғри киритдингиз. Қайта киритинг:")
        return
        
    await state.clear()
    await message.answer("Бош меню:", reply_markup=main_menu())

# ------------------- ASOSIY ISHGA TUSHIRISH -------------------
async def main():
    logger.info("🤖 Бот ишга тушди!")
    
    # Webhook ni o'chirib polling ni ishga tushirish
    # Server muhitida webhook ishlatish mumkin
    if os.environ.get('WEBHOOK_MODE', 'false').lower() == 'true':
        # Webhook mode
        webhook_url = os.environ.get('WEBHOOK_URL')
        await bot.set_webhook(webhook_url)
        logger.info(f"✅ Webhook установлен: {webhook_url}")
        
        # aiohttp server ishga tushirish
        app = web.Application()
        app.router.add_post('/webhook', handle_webhook)
        
        runner = web.AppRunner(app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get('PORT', 3000)))
        await site.start()
        logger.info("✅ HTTP сервер запущен")
        
        # Server ni kutib turish
        await asyncio.Event().wait()
    else:
        # Polling mode (local ishlatish uchun)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

async def handle_webhook(request):
    try:
        data = await request.json()
        update = Update(**data)
        await dp.feed_update(bot, update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"❌ Webhook обработкасида хato: {e}")
        return web.Response(status=500)

if __name__ == "__main__":
    # Graceful shutdown ni qo'llab-quvvatlash
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⚠️ Бот тўхтатилди")
    except Exception as e:
        logger.error(f"❌ Асосий функцияда хato: {e}")