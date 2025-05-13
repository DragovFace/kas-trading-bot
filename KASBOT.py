```python
import ccxt
import time
import json
import os
import logging
import telebot
import threading
import concurrent.futures
import queue
import signal
import sys
from functools import wraps
from threading import Lock

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Настраиваем консольный обработчик
for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))

# Настройки бота
profit_percent = 0.5 / 100  # 0.5%
drop_percent = 1 / 100      # 1%
delay_seconds = 30          # 30 секунд (для создания нового autobay)
order_size = 15             # $15
state_file = "bot_state.json"

# Глобальные переменные
notification_queue = queue.Queue()
command_queue = queue.Queue()
state_lock = Lock()
insufficient_funds_notified = False
trading_stopped = False  # Флаг остановки трейдинга
last_price = None
last_price_time = 0
price_cache_duration = 5

# Загрузка конфигурации из переменных окружения
config = {
    'exchange': {
        'api_key': os.getenv('EXCHANGE_API_KEY'),
        'secret': os.getenv('EXCHANGE_SECRET'),
    },
    'telegram': {
        'token': os.getenv('TELEGRAM_TOKEN'),
        'chat_id': os.getenv('TELEGRAM_CHAT_ID'),
    }
}

# Проверка, что все переменные окружения заданы
for section, keys in config.items():
    for key, value in keys.items():
        if value is None:
            raise ValueError(f"Переменная окружения {key.upper()} не задана")

# Инициализация клиента MEXC
exchange = ccxt.mexc({
    'apiKey': config['exchange']['api_key'],
    'secret': config['exchange']['secret'],
})

# Инициализация Telegram-бота
bot = telebot.TeleBot(config['telegram']['token'])
CHAT_ID = config['telegram']['chat_id']

# Декоратор для бесконечного переподключения
def retry_on_network_error(delay=5, max_delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except ccxt.NetworkError as e:
                    attempt += 1
                    current_delay = min(delay * (2 ** min(attempt, 10)), max_delay)
                    logging.warning(f"[NETWORK] Сетевая ошибка в {func.__name__}: {e}. Попытка {attempt}. Ожидаем {current_delay:.2f} сек")
                    time.sleep(current_delay)
                except Exception as e:
                    raise e
        return wrapper
    return decorator

# Функции работы с балансом и ценами
@retry_on_network_error()
def get_current_price(symbol):
    global last_price, last_price_time
    current_time = time.time()
    if last_price is None or current_time - last_price_time > price_cache_duration:
        last_price = exchange.fetch_ticker(symbol)['last']
        last_price_time = current_time
    return last_price

@retry_on_network_error()
def get_available_balance(currency):
    balance = exchange.fetch_balance()
    return balance.get(currency, {}).get('free', 0)

# Сохранение и загрузка состояния
def save_state(active_orders, executed_orders_count, total_profit):
    global trading_stopped
    with state_lock:
        if not active_orders and executed_orders_count == 0 and total_profit == 0 and not trading_stopped:
            logging.warning("[WARNING] Не записываем state — данные пустые")
            return
        new_state = {
            'active_orders': active_orders,
            'executed_orders_count': executed_orders_count,
            'total_profit': total_profit,
            'trading_stopped': trading_stopped
        }
        if os.path.exists(state_file):
            try:
                with open(state_file, "r", encoding='utf-8') as f:
                    current_state = json.load(f)
            except json.JSONDecodeError:
                logging.error("[ERROR] Ошибка чтения JSON. Файл повреждён.")
                current_state = {}
            except Exception as e:
                logging.error(f"[ERROR] Неизвестная ошибка при чтении JSON: {e}")
                current_state = {}
        else:
            current_state = {}
        if current_state == new_state:
            return
        try:
            with open(state_file, "w", encoding='utf-8') as f:
                json.dump(new_state, f, indent=4)
            logging.info("[SAVE] Состояние бота сохранено.")
        except Exception as e:
            logging.error(f"[ERROR] Ошибка при сохранении состояния: {e}")

def load_state():
    global trading_stopped
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding='utf-8') as f:
                state = json.load(f)
            trading_stopped = state.get('trading_stopped', False)
            logging.info(f"[LOAD] Состояние успешно загружено из bot_state.json, trading_stopped={trading_stopped}")
            return (
                state.get('active_orders', []),
                state.get('executed_orders_count', 0),
                state.get('total_profit', 0)
            )
        except json.JSONDecodeError as e:
            logging.error(f"[ERROR] Ошибка чтения bot_state.json: {e}. Возвращаем пустое состояние.")
            return [], 0, 0
        except Exception as e:
            logging.error(f"[ERROR] Неизвестная ошибка при загрузке состояния: {e}. Возвращаем пустое состояние.")
            return [], 0, 0
    logging.info("[LOAD] Файл bot_state.json не существует. Возвращаем пустое состояние.")
    trading_stopped = False
    return [], 0, 0

# Управление уведомлениями
def save_notification(notification_message):
    notification_queue.put(notification_message)
    logging.info(f"[NOTIFY] Уведомление добавлено: {notification_message}")

def send_notifications():
    while True:
        try:
            message = notification_queue.get(timeout=5)
            bot.send_message(CHAT_ID, message, timeout=10)
            notification_queue.task_done()
            logging.info("[NOTIFY] Уведомление отправлено")
        except queue.Empty:
            continue
        except Exception as e:
            logging.error(f"[ERROR] Ошибка при отправке уведомления: {e}")
            time.sleep(5)

# Функции работы с ордерами
@retry_on_network_error()
def create_buy_order(symbol, amount):
    global insufficient_funds_notified
    for attempt in range(3):
        try:
            order = exchange.create_market_buy_order(symbol, amount)
            actual_price = exchange.fetch_order(order['id'], symbol)['average']
            logging.info(f"[BUY] Куплено {amount} по {actual_price}")
            insufficient_funds_notified = False
            return order, actual_price
        except ccxt.InsufficientFunds as e:
            logging.error(f"[ERROR] Недостаточно средств для покупки: {e}")
            if not insufficient_funds_notified:
                save_notification(f"[ERROR] Недостаточно средств для покупки")
                insufficient_funds_notified = True
            return None, None
        except ccxt.RateLimitExceeded as e:
            logging.warning(f"[RATE] Превышен лимит запросов: {e}. Ожидаем...")
            time.sleep(2 ** attempt)
        except ccxt.OrderNotFound as e:
            logging.error(f"[ERROR] Ордер не найден: {e}")
            save_notification(f"[ERROR] Ордер не найден")
            return None, None
        except Exception as e:
            logging.error(f"[ERROR] Неизвестная ошибка покупки: {e}")
            save_notification(f"[ERROR] Ошибка покупки: {e}")
            return None, None
    logging.error("[ERROR] Не удалось создать ордер после 3 попыток")
    return None, None

@retry_on_network_error()
def create_sell_order(symbol, amount, price):
    global insufficient_funds_notified
    for attempt in range(3):
        try:
            order = exchange.create_limit_sell_order(symbol, amount, price)
            logging.info(f"[SELL] Ордер на продажу {amount} по {price} создан.")
            insufficient_funds_notified = False
            return order
        except ccxt.InsufficientFunds as e:
            logging.error(f"[ERROR] Недостаточно средств для продажи: {e}")
            if not insufficient_funds_notified:
                save_notification(f"[ERROR] Недостаточно средств для продажи")
                insufficient_funds_notified = True
            return None
        except ccxt.RateLimitExceeded as e:
            logging.warning(f"[RATE] Превышен лимит запросов: {e}. Ожидаем...")
            time.sleep(2 ** attempt)
        except ccxt.OrderNotFound as e:
            logging.error(f"[ERROR] Ордер не найден: {e}")
            save_notification(f"[ERROR] Ордер не найден")
            return None
        except Exception as e:
            logging.error(f"[ERROR] Ошибка продажи: {e}")
            save_notification(f"[ERROR] Ошибка продажи: {e}")
            return None
    logging.error("[ERROR] Не удалось создать ордер после 3 попыток")
    return None

def create_new_order(symbol, active_orders, executed_orders_count, total_profit, order_type):
    global insufficient_funds_notified
    exchange.load_markets()
    current_price = get_current_price(symbol)
    base_currency = symbol.split('/')[1]
    available_balance = get_available_balance(base_currency)

    if available_balance < order_size:
        if not insufficient_funds_notified:
            msg = f"[ERROR] Недостаточно средств ({base_currency}): {available_balance:.2f} USDT"
            logging.warning(msg)
            save_notification(msg)
            insufficient_funds_notified = True
        return active_orders, executed_orders_count, total_profit

    insufficient_funds_notified = False
    amount = order_size / current_price
    buy_order, actual_buy_price = create_buy_order(symbol, amount)
    if not buy_order:
        return active_orders, executed_orders_count, total_profit

    bought_amount = buy_order['amount']
    sell_price = actual_buy_price * (1 + profit_percent)
    save_notification(
        f"[BUY] КУПЛЕНО\n{bought_amount:.6f} KAS по {actual_buy_price:.6f} USDT\n"
        f"Потрачено: {bought_amount * actual_buy_price:.2f} USDT\n"
        f"[SELL] ВЫСТАВЛЕНО\n{bought_amount:.6f} KAS по {sell_price:.6f} USDT"
    )

    sell_order = create_sell_order(symbol, bought_amount, sell_price)
    if sell_order:
        active_orders.append({
            'id': sell_order['id'],
            'amount': bought_amount,
            'price': sell_price,
            'buy_price': actual_buy_price,
            'order_type': order_type
        })
    else:
        logging.error("[ERROR] Не удалось создать ордер на продажу.")
        save_notification("[ERROR] Не удалось создать ордер на продажу.")

    return active_orders, executed_orders_count, total_profit

def check_orders(symbol, active_orders, executed_orders_count, total_profit, initial_check=False):
    global trading_stopped
    logging.debug("[CHECK] Проверка статуса ордеров...")
    if initial_check:
        save_notification("[CHECK] Проверка статуса ранее созданных ордеров...")

    def fetch_order_status(order):
        time.sleep(0.2)
        try:
            return order, exchange.fetch_order(order['id'], symbol)
        except Exception as e:
            logging.error(f"[ERROR] Ошибка проверки ордера {order.get('id', 'N/A')}: {e}")
            return order, None

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(fetch_order_status, active_orders))
    except Exception as e:
        logging.error(f"[ERROR] Ошибка в ThreadPoolExecutor: {e}")
        return active_orders, executed_orders_count, total_profit

    try:
        updated_orders = []
        closed_orders = []
        canceled_orders = []
        autobay_needed = False
        flagautobay = 0

        for order, order_status in results:
            if not order_status:
                updated_orders.append(order)
                continue
            if order_status['status'] == 'closed':
                closed_orders.append(order)
                if order.get('order_type') == 'autobay':
                    autobay_needed = True
            elif order_status['status'] == 'canceled':
                canceled_orders.append(order)
                if order.get('order_type') == 'autobay':
                    autobay_needed = True
            else:
                if order.get('order_type') == 'autobay':
                    flagautobay += 1
                updated_orders.append(order)

        for order in closed_orders:
            buy_cost = order['buy_price'] * order['amount']
            sell_revenue = order['price'] * order['amount']
            profit = sell_revenue - buy_cost
            executed_orders_count += 1
            total_profit += profit
            save_notification(
                f"✅ Ордер выполнен!\n"
                f"Тип: {order.get('order_type', 'N/A')}\n"
                f"Количество: {order['amount']:.6f} KAS\n"
                f"Покупка: {order['buy_price']:.6f} USDT\n"
                f"Продажа: {order['price']:.6f} USDT\n"
                f"Прибыль: {profit:.6f} USD"
            )
            logging.info(
                f"[SUCCESS] Ордер выполнен: Тип={order.get('order_type', 'N/A')}, "
                f"Количество={order['amount']:.6f} KAS, Покупка={order['buy_price']:.6f} USDT, "
                f"Продажа={order['price']:.6f} USDT, Прибыль={profit:.6f} USD"
            )

        for order in canceled_orders:
            logging.info(f"[CANCEL] Ордер {order['id']} отменен.")
            save_notification(f"[CANCEL] Ордер {order['id']} отменен.")

        if initial_check and not updated_orders:
            if not trading_stopped:
                logging.info("[START] Ордеров нет. Создаем 'autobay'.")
                return create_new_order(symbol, updated_orders, executed_orders_count, total_profit, "autobay")
            else:
                logging.info("[START] Ордеров нет, но трейдинг остановлен. Не создаем 'autobay'.")
                return updated_orders, executed_orders_count, total_profit

        if not trading_stopped:
            if flagautobay == 0 and updated_orders:
                logging.info("[UPDATE] Назначаем новый 'autobay'.")
                updated_orders.sort(key=lambda x: x.get('price', float('inf')))
                updated_orders[0]['order_type'] = 'autobay'
            elif flagautobay > 1:
                logging.info("[WARNING] Несколько 'autobay'. Оставляем самый дешевый.")
                updated_orders.sort(key=lambda x: x.get('price', float('inf')))
                first_autobay = True
                for order in updated_orders:
                    if order.get('order_type') == 'autobay':
                        if first_autobay:
                            first_autobay = False
                        else:
                            order['order_type'] = 'bay'

        save_state(updated_orders, executed_orders_count, total_profit)

        if autobay_needed and not initial_check and not trading_stopped:
            time.sleep(delay_seconds)
            updated_orders, executed_orders_count, total_profit = create_new_order(
                symbol, updated_orders, executed_orders_count, total_profit, "autobay"
            )
            save_state(updated_orders, executed_orders_count, total_profit)

        if initial_check:
            save_notification("Проверка ранее созданных ордеров завершена.")

        return updated_orders, executed_orders_count, total_profit
    except Exception as e:
        logging.error(f"[ERROR] Неизвестная ошибка в check_orders: {e}")
        return active_orders, executed_orders_count, total_profit

# Основная функция бота
def run_bot(symbol):
    global insufficient_funds_notified, trading_stopped
    logging.info(f"[START] Запуск бота для {symbol}")
    active_orders, executed_orders_count, total_profit = load_state()
    active_orders, executed_orders_count, total_profit = check_orders(
        symbol, active_orders, executed_orders_count, total_profit, initial_check=True
    )
    save_state(active_orders, executed_orders_count, total_profit)

    while True:
        try:
            command = command_queue.get_nowait()
            if command == "stop":
                logging.info("[STOP] Трейдинг остановлен.")
                trading_stopped = True
                for order in active_orders:
                    if order.get("order_type") == "autobay":
                        order["order_type"] = "bay"
                        save_state(active_orders, executed_orders_count, total_profit)
                        save_notification(f"[STOP] Трейдинг остановлен. Ордер {order['id']} переведен в 'bay'.")
                        break
                else:
                    save_notification("[STOP] Ордеров 'autobay' нет.")
                save_state(active_orders, executed_orders_count, total_profit)
            elif command == "start":
                logging.info("[START] Трейдинг запущен.")
                trading_stopped = False
                insufficient_funds_notified = False
                active_orders, executed_orders_count, total_profit = check_orders(
                    symbol, active_orders, executed_orders_count, total_profit, initial_check=True
                )
                save_state(active_orders, executed_orders_count, total_profit)
            elif command == "buy":
                active_orders, executed_orders_count, total_profit = create_new_order(
                    symbol, active_orders, executed_orders_count, total_profit, "bay"
                )
                save_state(active_orders, executed_orders_count, total_profit)
                save_notification("[BUY] Создан ордер 'bay'.")
        except queue.Empty:
            pass

        active_orders, executed_orders_count, total_profit = check_orders(
            symbol, active_orders, executed_orders_count, total_profit
        )
        active_orders.sort(key=lambda x: x.get('price', float('inf')))
        save_state(active_orders, executed_orders_count, total_profit)

        autobay_order = next((o for o in active_orders if o.get("order_type") == "autobay"), None)
        current_price = get_current_price(symbol)
        if autobay_order and current_price <= autobay_order['buy_price'] * (1 - drop_percent) and not trading_stopped:
            base_currency = symbol.split('/')[1]
            available_balance = get_available_balance(base_currency)
            if available_balance >= order_size:
                logging.info(f"[UPDATE] Цена упала до {current_price:.6f}. Заменяем autobay.")
                autobay_order['order_type'] = "bay"
                active_orders, executed_orders_count, total_profit = create_new_order(
                    symbol, active_orders, executed_orders_count, total_profit, "autobay"
                )
                save_state(active_orders, executed_orders_count, total_profit)
            else:
                if not insufficient_funds_notified:
                    msg = f"[ERROR] Недостаточно средств ({base_currency}): {available_balance:.2f} USDT"
                    logging.warning(msg)
                    save_notification(msg)
                    insufficient_funds_notified = True

# Telegram-обработчики
@bot.message_handler(commands=['stop'])
def stop_trading(message):
    bot.reply_to(message, "[STOP] Останавливаю трейдинг...")
    command_queue.put("stop")

@bot.message_handler(commands=['start'])
def start_trading(message):
    bot.reply_to(message, "[START] Запускаю трейдинг...")
    command_queue.put("start")

@bot.message_handler(commands=['stats'])
def send_stats(message):
    active_orders, executed_orders_count, total_profit = load_state()
    stats_message = (
        f"[STATS] Статистика:\n"
        f"Выполнено ордеров: {executed_orders_count}\n"
        f"Общая сумма прибыли: {total_profit:.6f} USD\n"
        f"Активных ордеров: {len(active_orders)}\n"
    )
    bot.reply_to(message, stats_message)

@bot.message_handler(commands=['balance'])
def send_balance(message):
    try:
        current_price = get_current_price("KAS/USDT")
        active_orders, executed_orders_count, total_profit = load_state()
        usdt_balance = get_available_balance("USDT")
        kas_balance = get_available_balance("KAS")
        frozen_usdt = sum(order["amount"] * order["price"] for order in active_orders)
        frozen_kas = sum(order["amount"] for order in active_orders)
        total_sell_value = sum(order["amount"] * current_price for order in active_orders)
        PnL = ((total_sell_value + usdt_balance) / (frozen_usdt + usdt_balance - total_profit) - 1) * 100
        balance_message = (
            "<u>БАЛАНС</u>\n\n"
            f"<b>USDT</b>\n"
            f"Доступно: {usdt_balance:.2f} USDT\n"
            f"Заморожено: {frozen_usdt:.2f} USDT\n\n"
            f"<b>KAS</b>\n"
            f"Доступно: {kas_balance:.2f} KAS\n"
            f"Заморожено: {frozen_kas:.2f} KAS\n\n"
            f"<b>Ордера:</b>\n"
            f"Количество: {len(active_orders)}\n"
            f"Сумма исполнения: {total_sell_value:.2f} USDT\n\n"
            f"PnL: {PnL:.2f}%"
        )
        bot.reply_to(message, balance_message, parse_mode="HTML")
    except Exception as e:
        logging.error(f"[ERROR] Ошибка при получении баланса: {e}")
        bot.reply_to(message, "[ERROR] Произошла ошибка при получении баланса.")

@bot.message_handler(commands=['price'])
def send_price(message):
    try:
        current_price = get_current_price("KAS/USDT")
        price_message = f"[PRICE] Текущая цена KAS: {current_price:.6f} USDT"
        bot.reply_to(message, price_message)
    except Exception as e:
        logging.error(f"[ERROR] Ошибка при получении цены: {e}")
        bot.reply_to(message, "[ERROR] Произошла ошибка при получении текущей цены.")

@bot.message_handler(commands=['buy'])
def buy_order(message):
    bot.reply_to(message, "[BUY] Запрос на создание ордера 'bay' принят.")
    command_queue.put("buy")

# Обработка сигналов
def handle_exit(signum, frame):
    logging.info("[EXIT] Бот остановлен вручную. Состояние сохранено.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

# Запуск
if __name__ == "__main__":
    symbol = "KAS/USDT"
    threading.Thread(target=run_bot, args=(symbol,), daemon=True).start()
    threading.Thread(target=send_notifications, daemon=True).start()
    bot.polling()
