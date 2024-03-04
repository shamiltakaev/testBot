from bsonstream import KeyValueBSONInput

from datetime import datetime
from dateutil.relativedelta import relativedelta
import telebot
import json

def aggr(dt_from, dt_upto, group_type):
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)
    file = "sample_collection.bson"
    f=None
    f = open(file, 'rb')
    stream = KeyValueBSONInput(fh=f)
    
    dates = list(filter(lambda item: dt_from.replace(tzinfo=item["dt"].tzinfo) <= item["dt"] <= dt_upto.replace(tzinfo=item["dt"].tzinfo), stream))
    res_d = {}
    while dt_from <= dt_upto:
        res_d[dt_from.isoformat()] = 0
        if group_type == "day":
            da = list(filter(lambda item: datetime.date(item["dt"]) == dt_from, dates))
            for d in da:
                res_d[dt_from.isoformat()] += d["value"]

            dt_from = dt_from + relativedelta(days=1)
        elif group_type == "month":
            da = list(filter(lambda item: 
                            item["dt"].month == dt_from.month and 
                            item["dt"].year == dt_from.year, dates))

            for d in da:
                res_d[dt_from.isoformat()] += d["value"]

            dt_from += relativedelta(months=1)
        elif group_type == "hour":
            da = list(filter(lambda item: 
                            item["dt"].hour == dt_from.hour and
                            item["dt"].day == dt_from.day and
                            item["dt"].month == dt_from.month and 
                            item["dt"].year == dt_from.year, dates))
            for d in da:
                res_d[dt_from.isoformat()] += d["value"]

            dt_from += relativedelta(hours=1)
    return res_d


bot = telebot.TeleBot(token="5763251338:AAHr90ae4JpABIwxTEI1vrFlkVqjnj3cBdA")

@bot.message_handler(commands=['start'])
def start_bot(message):
    bot.send_message(message.chat.id, f"""Привет <a href="tg://user?id={message.from_user.id}">{message.from_user.username}</a>, это бот для разных вещей""", parse_mode="HTML")


@bot.message_handler(content_types=['text', 'document', 'audio'])
def get_json(message: telebot.types.Message):
    d = json.loads(message.text)
    d = aggr(**d)
    d = {
        "dataset": list(d.values()),
        "labels": list(d.keys())
    }
    bot.send_message(message.chat.id, json.dumps(d))



bot.polling()
