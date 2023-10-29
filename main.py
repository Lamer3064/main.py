from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# тг
bot_token = '6600558605:AAHpdpY7dQuBosDoHID4Znc4ibs1e_59f5c'
bot = Bot(token=bot_token)
dp = Dispatcher(bot)

# монго
client = MongoClient('localhost', 27017)
db = client['sample_database']
collection = db.sample_collection

def aggregate_data_m(dt_from, dt_upto):
    dt_from = dt_from.replace(hour=0, minute=0, second=0, microsecond=0)
    dt_upto = dt_upto.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": dt_from,
                    "$lt": dt_upto
                }
            }
        },
        {
            "$group": {
                "_id": {"$month": "$dt"},
                "total": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    results = collection.aggregate(pipeline)
    dataset = [r["total"] for r in results]

    labels = []
    current_date = dt_from
    while current_date < dt_upto:
        labels.append(current_date.isoformat())
        current_date += relativedelta(months=1)

    return {"dataset": dataset, "labels": labels}

def aggregate_data_h(dt_from, dt_upto):
    dt_from = dt_from.replace(microsecond=0)
    dt_upto = dt_upto.replace(microsecond=0)

    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": dt_from,
                    "$lte": dt_upto
                }
            }
        },
        {
            "$group": {
                "_id": {"$hour": "$dt"},
                "total": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    results = collection.aggregate(pipeline)
    dataset = [r["total"] for r in results]

    last_hour_data = next((item for item in results if item["_id"] == dt_upto.hour), None)
    if last_hour_data is None:
        dataset.append(0)
    else:
        dataset.append(last_hour_data["total"])

    # Создаем полный набор меток времени
    labels = [dt_from + timedelta(hours=i) for i in range(int((dt_upto - dt_from).total_seconds() // 3600) + 1)]

    # Выравниваем результаты с метками времени
    aligned_dataset = []
    for label in labels:
        aligned_dataset.append(next((item for item, label_item in zip(dataset, labels) if label_item == label), 0))

    return {"dataset": aligned_dataset, "labels": [label.isoformat() for label in labels]}

def aggregate_data_d(dt_from, dt_upto):
    dt_from = dt_from.replace(microsecond=0)
    dt_upto = dt_upto.replace(microsecond=0)

    pipeline = [
        {
            "$match": {
                "dt": {
                    "$gte": dt_from,
                    "$lt": dt_upto
                }
            }
        },
        {
            "$group": {
                "_id": {"day": {"$dayOfMonth": "$dt"}, "month": {"$month": "$dt"}, "year": {"$year": "$dt"}},
                "total": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1}
        }
    ]

    results = dict((tuple(r["_id"].values()), r["total"]) for r in collection.aggregate(pipeline))

    dataset = []
    labels = []
    current_date = dt_from
    while current_date <= dt_upto:
        labels.append(current_date.isoformat())
        dataset.append(results.get((current_date.day, current_date.month, current_date.year), 0))
        current_date += timedelta(days=1)

    return {"dataset": dataset, "labels": labels}

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.answer("Привет! Отправьте мне данные в формате JSON.")

@dp.message_handler()
async def handle_json(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = datetime.strptime(data["dt_from"], "%Y-%m-%dT%H:%M:%S")
        dt_upto = datetime.strptime(data["dt_upto"], "%Y-%m-%dT%H:%M:%S")
        group_type = data["group_type"]

        if group_type == 'month':
            result = aggregate_data_m(dt_from, dt_upto)
        elif group_type == 'hour':
            result = aggregate_data_h(dt_from, dt_upto)
        elif group_type == 'day':
            result = aggregate_data_d(dt_from, dt_upto)

        await message.answer(json.dumps(result))
    except json.JSONDecodeError:
        await message.answer("Пожалуйста, отправьте мне данные в формате JSON.")

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)