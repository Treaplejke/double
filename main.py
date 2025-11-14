import telebot
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from telebot import types
import time
import os
import json
from datetime import datetime
from flask import Flask, jsonify
from threading import Thread, Lock, Timer
import itertools
# ▼▼▼ ДОБАВЛЕНО ДЛЯ /statshero ▼▼▼
from collections import Counter
# ▲▲▲ КОНЕЦ ▲▲▲

# ========== НАСТРОЙКИ ==========

TOKEN = os.getenv('TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID', '0'))
RATING_CHANGE = 25
DATABASE_URL = os.getenv('DATABASE_URL')

if not all([TOKEN, ADMIN_ID, DATABASE_URL]):
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!!! ОШИБКА: Не все переменные окружения установлены!")
    print("!!! Убедись, что TOKEN, ADMIN_ID и DATABASE_URL заданы в Render.")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

# ===== FLASK СЕРВЕР ДЛЯ UPTIME ROBOT =====
app = Flask(__name__)

@app.route('/', methods=['GET'])
def health():
    return jsonify({"status": "online", "message": "Bot is running"}), 200

@app.route('/ping', methods=['GET'])
def ping():
    return "pong", 200

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"server": "online", "bot": "active", "db": "connected"}), 200

# ===== НОВЫЙ ПУЛ СОЕДИНЕНИЙ (ПОТОКОБЕЗОПАСНЫЙ) =====
try:
    db_pool = ThreadedConnectionPool( 
        1, 5, dsn=DATABASE_URL, sslmode='require' 
    )
    print("✅ [DB POOL] Потокобезопасный пул соединений (Threaded) успешно создан.")
except Exception as e:
    print(f"🔥🔥🔥 [DB POOL] НЕ УДАЛОСЬ СОЗДАТЬ ПУЛ СОЕДИНЕНИЙ: {e}")
    db_pool = None

# ===== TELEGRAM БОТ =====
bot = telebot.TeleBot(TOKEN, parse_mode='HTML', disable_web_page_preview=True)

# ===== УПРАВЛЕНИЕ БАЗОЙ ДАННЫХ (ПУЛ) =====

def get_db_conn():
    """Берет соединение из пула."""
    if not db_pool:
        print("❌ [DB POOL] Пул не инициализирован.")
        return None
    try:
        return db_pool.getconn()
    except Exception as e:
        print(f"❌ [DB POOL] Не удалось получить соединение из пула: {e}")
        return None

def put_db_conn(conn):
    """Возвращает соединение в пул."""
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            print(f"❌ [DB POOL] Не удалось вернуть соединение в пул: {e}")

# ===== СЛОВАРЬ ПОЗИЦИЙ (Глобальный) =====
POSITIONS = {
    1: "Carry", 2: "Mid", 3: "Offlane",
    4: "Soft Support", 5: "Hard Support"
}

def get_player_positions_str(positions_list):
    """(Вспомогательная функция, не требует БД)"""
    if not positions_list:
        return "Не указано"
    pos_names = [POSITIONS.get(pos, "?") for pos in sorted(positions_list)]
    return ", ".join(pos_names)

# =========================================================================
# ========== НАЧАЛО БЛОКА: НОВЫЙ КЭШ ИГРОКОВ (PlayerCache) ==========
# =========================================================================

class PlayerCache:
    def __init__(self, refresh_interval=30):
        self.players = []  # Сам кэш
        self.lock = Lock() # Для потокобезопасности
        self.refresh_interval = refresh_interval
        self.last_updated = 0
        self._update_cache() # Первоначальное заполнение
        self._start_timer() # Запуск авто-обновления
        print("✅ [CACHE] Кэш игроков инициализирован.")

    def _start_timer(self):
        """Запускает таймер, который вызовет _auto_refresh."""
        self.timer = Timer(self.refresh_interval, self._auto_refresh)
        self.timer.daemon = True
        self.timer.start()

    def _auto_refresh(self):
        """Метод, вызываемый таймером для обновления кэша."""
        print("CACHE: [Auto-Refresh] Обновление кэша игроков...")
        self._update_cache()
        self._start_timer() # Сразу же планируем *следующее* обновление

    # ▼▼▼ ЗДЕСЬ БЫЛА ОШИБКА ОТСТУПА, ТЕПЕРЬ ИСПРАВЛЕНО ▼▼▼
    def _fetch_from_db(self):
        """
        Единственная функция, которая реально обращается к БД за 
        ПОЛНЫМ списком игроков.
        """
        conn = get_db_conn()
        if not conn:
            print("CACHE: [ERROR] Не удалось подключиться к БД для обновления кэша.")
            return None # Оставляем старые данные в кэше

        players_list = []
        try:
            with conn:
                with conn.cursor() as cur:
                    # 1. Получаем основные данные игроков
                    cur.execute('SELECT nickname, wins, losses, positions FROM players ORDER BY nickname')
                    players = cur.fetchall()
                    
                    # 2. Получаем РЕАЛЬНЫЕ роли (ТОЛЬКО ГДЕ ЕСТЬ ИГРЫ)
                    cur.execute('SELECT player_nickname, role_position FROM player_role_stats WHERE (wins + losses) > 0')
                    role_rows = cur.fetchall()
                    
                    # Группируем реальные роли
                    player_real_roles = {}
                    for nick, role_pos in role_rows:
                        if nick not in player_real_roles:
                            player_real_roles[nick] = []
                        player_real_roles[nick].append(role_pos)

                    # 3. Собираем итоговый список для кэша
                    for nickname, wins, losses, positions_json in players:
                        total = wins + losses
                        wr = round((wins / total * 100), 1) if total > 0 else 0
                        
                        # Берем роли из словаря реальных ролей
                        real_roles = player_real_roles.get(nickname, [])
                        
                        players_list.append({
                            'nickname': nickname,
                            'wr_str': f"{wr}%",
                            'pos_str': get_player_positions_str(real_roles) 
                        })
            return players_list
        except Exception as e:
            print(f"CACHE: [ERROR] Ошибка при чтении из БД: {e}")
            return None 
        finally:
            put_db_conn(conn)

    def _update_cache(self):
        """Потокобезопасно обновляет внутренний список игроков."""
        new_players = self._fetch_from_db()
        if new_players is not None:
            with self.lock:
                self.players = new_players
                self.last_updated = time.time()
            print(f"CACHE: [Success] Кэш обновлен. {len(self.players)} игроков.")

    def get_players(self):
        """Потокобезопасно получает список игроков из кэша."""
        with self.lock:
            # Возвращаем копию, чтобы ее нельзя было случайно изменить извне
            return self.players.copy() 

    def invalidate(self):
        """
        Принудительно и немедленно обновляет кэш.
        Вызывается после добавления/удаления игрока.
        """
        print("CACHE: [Invalidate] Принудительное обновление кэша...")
        # (Не нужен таймер, обновляем прямо сейчас)
        self._update_cache()

# --- Глобально создаем ОДИН объект кэша ---
player_cache = PlayerCache(refresh_interval=30) # Обновление каждые 30 сек

# =========================================================================
# ========== КОНЕЦ БЛОКА: НОВЫЙ КЭШ ИГРОКОВ (PlayerCache) ==========
# =========================================================================


# ===== СОЗДАНИЕ ТАБЛИЦ (ГАРАНТИРУЕМ, ЧТО ОНИ ЕСТЬ) =====
def create_tables():
    """Создает все таблицы, если они еще не существуют."""
    conn = get_db_conn()
    if not conn:
        print("❌ [DB INIT] Не могу создать таблицы. Нет подключения к БД.")
        return
        
    try:
        with conn: # Авто-commit или rollback
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS players (
                        id SERIAL PRIMARY KEY, nickname TEXT UNIQUE NOT NULL, rating INTEGER DEFAULT 0,
                        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, mmr INTEGER DEFAULT 0,
                        positions TEXT DEFAULT '[]', total_kills INTEGER DEFAULT 0,
                        total_deaths INTEGER DEFAULT 0, total_assists INTEGER DEFAULT 0
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS games (
                        id SERIAL PRIMARY KEY, screenshot_file_id TEXT, radiant_players TEXT,
                        dire_players TEXT, result TEXT, date TEXT, time TEXT, description TEXT
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS player_game_stats (
                        id SERIAL PRIMARY KEY, game_id INTEGER, player_nickname TEXT, hero TEXT,
                        kills INTEGER, deaths INTEGER, assists INTEGER, team TEXT, position INTEGER DEFAULT 0,
                        rating_delta INTEGER DEFAULT 0,
                        FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
                    )
                ''')
                cur.execute("""
                    ALTER TABLE player_game_stats
                    ADD COLUMN IF NOT EXISTS rating_delta INTEGER DEFAULT 0
                """)
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS player_heroes (
                        id SERIAL PRIMARY KEY, player_nickname TEXT, hero_name TEXT,
                        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, total_kills INTEGER DEFAULT 0,
                        total_deaths INTEGER DEFAULT 0, total_assists INTEGER DEFAULT 0,
                        UNIQUE(player_nickname, hero_name),
                        FOREIGN KEY (player_nickname) REFERENCES players(nickname) ON DELETE CASCADE
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS player_role_stats (
                        id SERIAL PRIMARY KEY, player_nickname TEXT, role_position INTEGER,
                        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
                        UNIQUE(player_nickname, role_position),
                        FOREIGN KEY (player_nickname) REFERENCES players(nickname) ON DELETE CASCADE
                    )
                ''')
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_activity (
                        id SERIAL PRIMARY KEY, user_id BIGINT UNIQUE NOT NULL, username TEXT,
                        first_name TEXT, last_name TEXT, first_visit TEXT, last_visit TEXT,
                        total_commands INTEGER DEFAULT 0
                    )
                ''')
        print("✅ [DB INIT] Таблицы успешно проверены/созданы в PostgreSQL.")
    except Exception as e:
        print(f"🔥🔥🔥 [DB INIT] Ошибка при создании таблиц: {e}")
    finally:
        put_db_conn(conn)

user_state = {}

def log_user_activity(user_id, message):
    conn = get_db_conn()
    if not conn: return
    try:
        with conn:
            with conn.cursor() as cur:
                username = message.from_user.username or "no_username"
                first_name = message.from_user.first_name or "Unknown"
                last_name = message.from_user.last_name or ""
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur.execute('SELECT total_commands FROM user_activity WHERE user_id=%s', (user_id,))
                row = cur.fetchone()
                if row:
                    total_commands = row[0] + 1
                    cur.execute(
                        '''UPDATE user_activity SET last_visit=%s, total_commands=%s WHERE user_id=%s''',
                        (now, total_commands, user_id)
                    )
                else:
                    cur.execute(
                        '''INSERT INTO user_activity (user_id, username, first_name, last_name, first_visit, last_visit, total_commands)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                        (user_id, username, first_name, last_name, now, now, 1)
                    )
    except Exception as e:
        print(f"Ошибка логирования: {e}")
    finally:
        put_db_conn(conn)


def is_admin(user_id):
    return user_id == ADMIN_ID


def get_leaderboard_text():
    conn = get_db_conn()
    if not conn: return "Ошибка подключения к БД."
    rows = []
    try:
        with conn:
            with conn.cursor() as cur:
                query = '''
                    SELECT 
                        nickname, rating, wins, losses, 
                        total_kills, total_deaths, total_assists,
                        (CASE 
                            WHEN (wins + losses) = 0 THEN 0 
                            ELSE (CAST(wins AS FLOAT) / (wins + losses)) * 100 
                        END) AS wr,
                        (CASE 
                            WHEN total_deaths = 0 THEN (total_kills + total_assists) 
                            ELSE (CAST(total_kills AS FLOAT) + total_assists) / total_deaths 
                        END) AS kda
                    FROM players 
                    ORDER BY 
                        rating DESC, 
                        kda DESC, 
                        wr DESC
                '''
                cur.execute(query)
                rows = cur.fetchall()
    except Exception as e:
        print(f"Ошибка get_leaderboard_text: {e}")
        return "Ошибка выполнения запроса к БД."
    finally:
        put_db_conn(conn)
    
    if not rows:
        return "📭 Лидерборд пуст."
    
    text = "🏆 ЛИДЕРБОРД\n" + "=" * 50 + "\n"
    
    # Список медалей для топ-5
    medals = ["🥇", "🥈", "🥉", "🏅", "🏅"] 
    
    for idx, (nickname, rating, wins, losses, total_kills, total_deaths, total_assists, wr, kda) in enumerate(rows, 1):
        
        if idx <= 5:
            # Места 1-5 (как и было)
            medal = medals[idx - 1]
        elif idx <= 10:
            # Места 6-10 (клоуны)
            medal = f"🤡 {idx}."
        else:
            # Все, кто ниже 10-го (коляски)
            medal = f"♿ {idx}."

        wr_str = f"{wr:.1f}"
        kda_str = f"{kda:.2f}"
        text += f"{medal} {nickname} - Рейтинг: {rating} | W/L: {wins}/{losses} | WR: {wr_str}% | KDA: {kda_str}\n"
    
    return text

# ----- УЛУЧШЕННЫЕ ФУНКЦИИ (ПРИНИМАЮТ `cur` ОБЪЕКТ) -----

def get_top_heroes(cur, nickname, limit=3):
    try:
        cur.execute('''
            SELECT hero_name, wins, losses, total_kills, total_deaths, total_assists 
            FROM player_heroes 
            WHERE player_nickname=%s AND (wins + losses) > 0
            ORDER BY (CAST(wins AS FLOAT) / (wins + losses)) DESC
            LIMIT %s
        ''', (nickname, limit))
        rows = cur.fetchall()
        heroes_text = ""
        if not rows: return "Нет данных"
        for idx, (hero_name, wins, losses, kills, deaths, assists) in enumerate(rows, 1):
            total = wins + losses
            wr = round((wins / total * 100), 1) if total > 0 else 0
            kda = round((kills + assists) / deaths, 2) if deaths > 0 else (kills + assists)
            heroes_text += f"{idx}. {hero_name} - W/L: {wins}/{losses} | WR: {wr}% | KDA: {kda}\n"
        return heroes_text
    except Exception as e:
        print(f"Ошибка get_top_heroes: {e}")
        return "Ошибка БД"

def get_role_stats(cur, nickname):
    try:
        cur.execute('''
            SELECT role_position, wins, losses 
            FROM player_role_stats 
            WHERE player_nickname=%s
            ORDER BY role_position
        ''', (nickname,))
        rows = cur.fetchall()
        if not rows: return "Нет данных по ролям"
        role_stats_text = ""
        for role_pos, wins, losses in rows:
            total = wins + losses
            
            # ▼▼▼ ДОБАВИТЬ ЭТУ ПРОВЕРКУ ▼▼▼
            if total == 0: continue # Пропускаем пустые роли
            # ▲▲▲ ------------------- ▲▲▲
            
            wr = round((wins / total * 100), 1) if total > 0 else 0 
            role_name = POSITIONS.get(role_pos, "Неизвестная")
            role_stats_text += f"    {role_name}: W/L {wins}/{losses} | WR {wr}%\n"
        return role_stats_text if role_stats_text else "Нет активных ролей"
    except Exception as e:
        print(f"Ошибка get_role_stats: {e}")
        return "Ошибка БД"

# ----- ГЛАВНАЯ ФУНКЦИЯ СТАТИСТИКИ (ИСПОЛЬЗУЕТ 1 СОЕДИНЕНИЕ) -----

def get_player_stats(nickname):
    conn = get_db_conn()
    if not conn: return None
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Получаем основные данные
                cur.execute(
                    'SELECT rating, wins, losses, mmr, positions, total_kills, total_deaths, total_assists '
                    'FROM players WHERE nickname=%s',
                    (nickname,)
                )
                row = cur.fetchone()
                if not row: return None
                rating, wins, losses, mmr, positions_json, total_kills, total_deaths, total_assists = row
                
                # 2. Получаем "предпочитаемые" позиции (оставляем как есть)
                try:
                    preferred_positions_list = json.loads(positions_json) if positions_json else []
                except Exception:
                    preferred_positions_list = []
                
                cur.execute(
                    'SELECT role_position FROM player_role_stats WHERE player_nickname=%s AND (wins + losses) > 0 ORDER BY (wins+losses) DESC', 
                    (nickname,)
                )
                roles_rows = cur.fetchall()
                actual_roles_list = [row[0] for row in roles_rows]

                # 4. Собираем остальную статистику
                total_games = wins + losses
                wr = round((wins / total_games * 100), 1) if total_games > 0 else 0
                avg_kda = round((total_kills + total_assists) / total_deaths, 2) if total_deaths > 0 else (
                        total_kills + total_assists)
                top_heroes_text = get_top_heroes(cur, nickname)
                role_stats_text = get_role_stats(cur, nickname)

        # 5. Возвращаем результат
        return {
            "nickname": nickname, "rating": rating, "wins": wins, "losses": losses,
            "total_games": total_games, "wr": wr, "mmr": mmr, 
            
            # "positions" (для балансировщика) - берем РЕАЛЬНЫЕ роли
            "positions": actual_roles_list, 
            
            # "positions_str" (для отображения) - берем РЕАЛЬНЫЕ роли
            # Команда /admin_set_positions теперь не влияет на отображение
            "positions_str": get_player_positions_str(actual_roles_list),
            
            # (Если хочешь вернуть старую логику, раскомментируй строку ниже и закомментируй "positions_str" выше)
            # "positions_str": get_player_positions_str(preferred_positions_list), 
            
            "avg_kda": avg_kda,
            "total_kda": f"{total_kills}/{total_deaths}/{total_assists}",
            "top_heroes": top_heroes_text, 
            "role_stats": role_stats_text
        }
    except Exception as e:
        print(f"Ошибка get_player_stats: {e}")
        return None
    finally:
        put_db_conn(conn)

# ------------------------------------------------------------------

def get_player_stats_text(data):
    text = f"📊 Профиль {data['nickname']}\n"
    text += f"Рейтинг: {data['rating']}\n"
    text += f"Всего игр: {data['total_games']}\n"
    text += f"Побед: {data['wins']}\n"
    text += f"Поражений: {data['losses']}\n"
    text += f"Win Rate: {data['wr']}%\n"
    text += f"MMR: {data['mmr']}\n"
    text += f"Позиции: {data['positions_str']}\n" # <--- Теперь показывает реальные роли
    text += f"KDA: {data['avg_kda']}\n\n"
    text += f"📈 Статистика по ролям:\n"
    text += data['role_stats'] + "\n\n"
    text += f"🎯 Топ герои:\n"
    text += data['top_heroes']
    return text

def get_all_games(limit=20):
    conn = get_db_conn()
    if not conn: return []
    rows = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT id, screenshot_file_id, radiant_players, dire_players, result, date, time, description '
                    'FROM games ORDER BY date DESC, time DESC LIMIT %s',
                    (limit,))
                rows = cur.fetchall()
    except Exception as e:
        print(f"Ошибка get_all_games: {e}")
    finally:
        put_db_conn(conn)
    return rows

# ▼▼▼ "УМНЫЙ" БАЛАНСИРОВЩИК v3 (Уже использует 'positions' из get_player_stats) ▼▼▼
# ▼▼▼ "УМНЫЙ" БАЛАНСИРОВЩИК v4 (Фикс композиции и весов) ▼▼▼
# ▼▼▼ "УМНЫЙ" БАЛАНСИРОВЩИК v5 (Критический лимит ролей) ▼▼▼
def balance_teams(selected_players):
    """
    УМНЫЙ БАЛАНС v5: Вводит жесткий лимит на количество игроков, 
    которые могут играть на одной позиции (максимум 3 из 5), 
    и сохраняет приоритет покрытия всех 5 слотов.
    """
    player_data = []
    for player in selected_players:
        data = get_player_stats(player)
        if data:
            player_data.append({
                'nickname': data['nickname'], 
                'mmr': data['mmr'], 
                'wr': data['wr'],
                'rating': data['rating'], 
                'pos_str': data['positions_str'],
                'positions': data['positions']  # Список реальных ролей [1, 2, 3]
            })
    
    if len(player_data) < 2: 
        return [], []

    team_size = len(player_data) // 2
    
    # --- 1. Рассчитываем общие и целевые показатели ---
    total_mmr = sum(p['mmr'] for p in player_data)
    total_wr = sum(p['wr'] for p in player_data)
    
    target_mmr = total_mmr / 2.0
    target_wr = total_wr / 2.0
    
    best_combination = None
    min_total_score = float('inf')

    # --- Веса для очков (Новая настройка) ---
    MAX_ROLE_PLAYERS = 3                # Максимум 3 игрока могут иметь одну позицию как реальную
    
    COMPOSITION_DEFICIT_WEIGHT = 1000.0 # Штраф за отсутствие 1/2/3/4/5
    ROLE_SATURATION_WEIGHT = 5000.0     # NEW: Критический штраф за переизбыток (> 3)
    MMR_WR_WEIGHT = 200.0               # Вес MMR/WR
    ROLE_CONFLICT_WEIGHT = 5.0          # Штраф за минимальный конфликт (2 или 3 игрока)
    REQUIRED_ROLES = {1, 2, 3, 4, 5}

    # --- 2. Перебираем все комбинации ---
    for team1_players in itertools.combinations(player_data, team_size):
        
        # --- 3. Считаем MMR/WR score ---
        team1_mmr_sum = sum(p['mmr'] for p in team1_players)
        team1_wr_sum = sum(p['wr'] for p in team1_players)
        
        mmr_diff = abs(team1_mmr_sum - target_mmr)
        wr_diff = abs(team1_wr_sum - target_wr)
        
        mmr_norm_diff = (mmr_diff / target_mmr) if target_mmr > 0 else 0
        wr_norm_diff = (wr_diff / target_wr) if target_wr > 0 else 0
        
        mmr_wr_score = (mmr_norm_diff + wr_norm_diff) if target_mmr > 0 and target_wr > 0 else (mmr_norm_diff or wr_norm_diff)
        
        # --- 4. Считаем Role Conflict score & Saturation Penalty ---
        all_roles = []
        for p in team1_players:
            player_roles = p['positions']
            if player_roles:
                all_roles.extend(player_roles)

        role_counts = Counter(all_roles)
        role_conflict_score = 0
        saturation_penalty = 0 # Новый, критичный штраф

        for role_id, role_count in role_counts.items():
            if role_count > MAX_ROLE_PLAYERS:
                # 🔴 КРИТИЧЕСКИЙ ШТРАФ: Если у нас 4 или 5 саппортов, это сразу делает команду неоптимальной
                saturation_penalty += (role_count - MAX_ROLE_PLAYERS) 
            
            if role_count > 1 and role_count <= MAX_ROLE_PLAYERS:
                 # Небольшой штраф за 2-3 игрока (это не идеальный состав, но допустимый)
                 role_conflict_score += (role_count - 1) 

        # --- 4.5. Считаем Composition Coverage score (ШТРАФ ЗА ОТСУТСТВИЕ СЛОТОВ) ---
        covered_roles = set(role_counts.keys()) 
        composition_coverage_penalty = len(REQUIRED_ROLES - covered_roles)
        
        # --- 5. Считаем итоговый score (Применяем веса) ---
        # SATURATION_WEIGHT > COMPOSITION_DEFICIT_WEIGHT > MMR/WR WEIGHT
        total_score = (ROLE_SATURATION_WEIGHT * saturation_penalty) + \
                      (COMPOSITION_DEFICIT_WEIGHT * composition_coverage_penalty) + \
                      (ROLE_CONFLICT_WEIGHT * role_conflict_score) + \
                      (MMR_WR_WEIGHT * mmr_wr_score)

        # --- 6. Ищем комбинацию с наименьшим "штрафом" ---
        if total_score < min_total_score:
            min_total_score = total_score
            best_combination = team1_players

    # --- 7. Формируем команды ---
    if best_combination is None:
        # Аварийный случай: делим пополам
        return player_data[:team_size], player_data[team_size:]

    radiant = list(best_combination)
    radiant_nicknames = {p['nickname'] for p in radiant}
    dire = [p for p in player_data if p['nickname'] not in radiant_nicknames]
    
    return radiant, dire
# ▲▲▲ КОНЕЦ v5 ▲▲▲
# ▲▲▲ КОНЕЦ v4 ▲▲▲
# ▲▲▲ КОНЕЦ v3 ▲▲▲

# ===== КОМАНДЫ БОТА =====

@bot.message_handler(commands=['start'])
def start(message):
    log_user_activity(message.from_user.id, message)
    text = "👋 Добро пожаловать в бот мониторинга Dota лиги!\n\nИспользуйте /help для списка команд."
    try: bot.reply_to(message, text)
    except Exception as e: print(f"Ошибка start: {e}")

@bot.message_handler(commands=['help'])
def help_command(message):
    log_user_activity(message.from_user.id, message)
    text = "📖 СПРАВКА ПО КОМАНДАМ\n\n"
    text += "/leaderboard - просмотр лидерборда 🏆\n"
    text += "/player nickname - статистика игрока 📊\n"
    text += "/games - все игры 🎮\n"
    text += "/creategame - создать игру 🎯\n"
    text += "/statshero - статистика по героям 🦸\n"
    try: bot.reply_to(message, text)
    except Exception as e: print(f"Ошибка help: {e}")

@bot.message_handler(commands=['leaderboard'])
def leaderboard(message):
    log_user_activity(message.from_user.id, message)
    text = get_leaderboard_text()
    conn = get_db_conn()
    if not conn:
        bot.reply_to(message, "Ошибка БД.")
        return
    rows = []
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT nickname FROM players ORDER BY rating DESC, (CASE WHEN total_deaths = 0 THEN (total_kills + total_assists) ELSE (CAST(total_kills AS FLOAT) + total_assists) / total_deaths END) DESC, (CASE WHEN (wins + losses) = 0 THEN 0 ELSE (CAST(wins AS FLOAT) / (wins + losses)) END) DESC')
                rows = cur.fetchall()
    except Exception as e:
        print(f"Ошибка leaderboard (получение кнопок): {e}")
    finally:
        put_db_conn(conn)
    markup = types.InlineKeyboardMarkup()
    for (nickname,) in rows:
        markup.add(types.InlineKeyboardButton(f"👤 {nickname}", callback_data=f"player_{nickname}"))
    try:
        bot.reply_to(message, text, reply_markup=markup)
    except Exception as e:
        print(f"Ошибка leaderboard: {e}")

@bot.message_handler(commands=['player'])
def player_stats(message):
    log_user_activity(message.from_user.id, message)
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Использование: /player nickname")
        return
    nickname = " ".join(parts[1:])
    data = get_player_stats(nickname)
    if not data:
        bot.reply_to(message, "❌ Игрок не найден.")
        return
    text = get_player_stats_text(data)
    try:
        bot.reply_to(message, text)
    except Exception as e:
        print(f"Ошибка player: {e}")

@bot.message_handler(commands=['games'])
def show_all_games(message):
    log_user_activity(message.from_user.id, message)
    games = get_all_games(limit=20)
    if not games:
        bot.reply_to(message, "❌ Нет игр в базе")
        return
    text = f"🎮 ПОСЛЕДНИЕ ИГРЫ\n" + "=" * 50 + "\n"
    text += f"Всего: {len(games)} игр\n\n"
   for idx, (game_id, sfid, r_pl, d_pl, result, date, time_str, desc) in enumerate(games, 1):
        r_emoji = "🟢" if result == "radiant" else "🔴"
        text += f"{idx}. {r_emoji} {result.upper()} WIN\n"
        text += f"    🟢 Radiant: {r_pl}\n"
        text += f"    🔴 Dire: {d_pl}\n"
        text += f"    📅 {date} ⏰ {time_str}\n"
        if desc and desc.strip():
            text += f"    📝 {desc.strip()}\n"
        text += "\n"
    try:
        bot.reply_to(message, text)
        for game_id, sfid, r_pl, d_pl, result, date, time_str, desc in games:
            if sfid:
                try: bot.send_photo(message.chat.id, sfid)
                except Exception: pass
    except Exception as e:
        print(f"Ошибка games: {e}")

@bot.message_handler(commands=['creategame'])
def create_game(message):
    log_user_activity(message.from_user.id, message)
    
    # --- ИСПОЛЬЗУЕМ КЭШ ---
    players_from_cache = player_cache.get_players()
    if not players_from_cache:
        bot.reply_to(message, "❌ Нет игроков в системе. Сначала добавьте игроков (через /admin).")
        return
    # ---------------------
        
    user_id = message.from_user.id
    user_state[user_id] = {"action": "selecting_players", "selected": []}

    markup = types.InlineKeyboardMarkup()
    for player in players_from_cache:
        # Берем готовые данные из кэша
        markup.add(
            types.InlineKeyboardButton(
                # (player['pos_str']) ТЕПЕРЬ ПОКАЗЫВАЕТ РЕАЛЬНЫЕ РОЛИ
                f"{player['nickname']} ({player['pos_str']}) | WR: {player['wr_str']}",
                callback_data=f"select_player_{player['nickname']}"
            )
        )
    markup.add(types.InlineKeyboardButton("✅ Готово - Создать матч", callback_data="create_match"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_create"))

    try:
        bot.send_message(
            message.chat.id,
            "🎯 Выберите игроков которые ПРИСУТСТВУЮТ:\n(Нажимайте на игроков)",
            reply_markup=markup
        )
    except Exception as e:
        print(f"Ошибка create_game: {e}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_player_"))
def select_player_for_game(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    nickname = call.data.replace("select_player_", "")
    state = user_state[user_id]
    if nickname in state["selected"]:
        state["selected"].remove(nickname)
    else:
        state["selected"].append(nickname)

    # --- ИСПОЛЬЗУЕМ КЭШ ---
    players_from_cache = player_cache.get_players()
    # ---------------------
    
    markup = types.InlineKeyboardMarkup()
    for player in players_from_cache:
        prefix = "✅ " if player['nickname'] in state["selected"] else ""
        markup.add(
            types.InlineKeyboardButton(
                f"{prefix}{player['nickname']} ({player['pos_str']}) | WR: {player['wr_str']}",
                callback_data=f"select_player_{player['nickname']}"
            )
        )
    markup.add(types.InlineKeyboardButton("✅ Готово - Создать матч", callback_data="create_match"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_create"))
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass

@bot.callback_query_handler(func=lambda call: call.data == "create_match")
def create_match(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    selected_players = state["selected"]
    if len(selected_players) < 2:
        try: bot.send_message(chat_id, "❌ Нужно выбрать минимум 2 игроков!")
        except Exception: pass
        return
    if len(selected_players) % 2 != 0:
        try: bot.send_message(chat_id, "❌ Количество игроков должно быть четным!")
        except Exception: pass
        return
    radiant, dire = balance_teams(selected_players) 
    
    text = "⚔️ СБАЛАНСИРОВАННЫЙ МАТЧ (по Ролям, MMR и WR)\n\n"
    text += "🟢 RADIANT:\n"
    radiant_total_wr = 0
    radiant_total_mmr = 0
    for p in radiant:
        # p['pos_str'] теперь тоже берется из РЕАЛЬНЫХ ролей
        text += f"    • {p['nickname']} ({p['pos_str']}) | WR: {p['wr']}% | MMR: {p['mmr']}\n"
        radiant_total_wr += p['wr']
        radiant_total_mmr += p['mmr']
    
    radiant_avg_wr = round(radiant_total_wr / len(radiant), 1) if radiant else 0
    radiant_avg_mmr = round(radiant_total_mmr / len(radiant), 0) if radiant else 0
    text += f"    ⭐ Средний WR: {radiant_avg_wr}%\n"
    text += f"    🎖️ Средний MMR: {int(radiant_avg_mmr)}\n\n"
    text += "🔴 DIRE:\n"
    dire_total_wr = 0
    dire_total_mmr = 0

    for p in dire:
        text += f"    • {p['nickname']} ({p['pos_str']}) | WR: {p['wr']}% | MMR: {p['mmr']}\n"
        dire_total_wr += p['wr']
        dire_total_mmr += p['mmr']

    dire_avg_wr = round(dire_total_wr / len(dire), 1) if dire else 0
    dire_avg_mmr = round(dire_total_mmr / len(dire), 0) if dire else 0
    text += f"    ⭐ Средний WR: {dire_avg_wr}%\n"
    text += f"    🎖️ Средний MMR: {int(dire_avg_mmr)}\n\n"
    text += f"📊 Баланс WR: {'ИДЕАЛЬНО ✅' if abs(radiant_avg_wr - dire_avg_wr) < 5 else 'ХОРОШИЙ 👍'}\n"
    text += f"📊 Баланс MMR: {'ИДЕАЛЬНО ✅' if abs(radiant_avg_mmr - dire_avg_mmr) < 50 else 'ХОРОШИЙ 👍'}\n"
    text += f"\n📈 WR Разница: {abs(radiant_avg_wr - dire_avg_wr):.1f}%\n"
    text += f"🎖️ MMR Разница: {int(abs(radiant_avg_mmr - dire_avg_mmr))}"
    try:
        bot.send_message(chat_id, text)
    except Exception as e:
        print(f"Ошибка create_match: {e}")
    del user_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data == "cancel_create")
def cancel_create(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    try: bot.edit_message_text("❌ Отменено.", chat_id, call.message.message_id)
    except Exception: pass
    if user_id in user_state: del user_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("player_"))
def show_player_profile(call):
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    nickname = call.data.replace("player_", "")
    data = get_player_stats(nickname)
    if not data:
        try: bot.send_message(call.message.chat.id, "❌ Игрок не найден.")
        except Exception: pass
        return
    text = get_player_stats_text(data)
    try:
        bot.send_message(call.message.chat.id, text)
    except Exception as e:
        print(f"Ошибка show_player_profile: {e}")

# ========== АДМИНИСТРАТОРСКИЕ КОМАНДЫ ==========

# ▼▼▼ ИСПРАВЛЕНИЕ 3.1: НОВАЯ ГЛАВНАЯ ФУНКЦИЯ АДМИНКИ ▼▼▼
def show_admin_panel(chat_id, user_id, message_id=None):
    """
    Отправляет или редактирует сообщение, показывая главную админ-панель.
    """
    if not is_admin(user_id): return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("➕ Добавить игрока", callback_data="admin_add_player"))
    markup.add(types.InlineKeyboardButton("🎮 Добавить игру", callback_data="admin_add_game"))
    markup.add(types.InlineKeyboardButton("↩️ Отменить последнюю игру", callback_data="admin_undo_game"))
    markup.add(types.InlineKeyboardButton("⚔️ Добавить героя игроку", callback_data="admin_add_hero"))
    markup.add(types.InlineKeyboardButton("📊 Управление ролями", callback_data="admin_manage_roles"))
    markup.add(types.InlineKeyboardButton("📝 Изменить рейтинг", callback_data="admin_set_rating"))
    markup.add(types.InlineKeyboardButton("🎖️ Добавить MMR", callback_data="admin_add_mmr"))
    markup.add(types.InlineKeyboardButton("🎯 Установить позиции", callback_data="admin_set_positions"))
    markup.add(types.InlineKeyboardButton("📋 Список игроков", callback_data="admin_list"))
    markup.add(types.InlineKeyboardButton("👥 Статистика пользователей", callback_data="admin_user_stats"))
    markup.add(types.InlineKeyboardButton("🗑️ Удалить игрока", callback_data="admin_delete_player"))
    
    text = "⚙️ Панель администратора\nВыберите действие:"
    
    try:
        if message_id:
            # Если мы "вернулись" из меню, редактируем старое сообщение
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        else:
            # Если это первая команда /admin, отправляем новое
            bot.send_message(chat_id, text, reply_markup=markup)
    except Exception as e:
        print(f"Ошибка в show_admin_panel: {e}")
# ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.1 ▲▲▲

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    if not is_admin(message.from_user.id):
        try: bot.reply_to(message, "❌ Доступ запрещён.")
        except Exception: pass
        return
    # ▼▼▼ ИСПРАВЛЕНИЕ 3.2: УПРОЩАЕМ ВЫЗОВ /admin ▼▼▼
    show_admin_panel(message.chat.id, message.from_user.id)
    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.2 ▲▲▲

# ▼▼▼ ИСПРАВЛЕНИЕ 3.3: НОВЫЙ ОБРАБОТЧИК КНОПКИ "НАЗАД" ▼▼▼
@bot.callback_query_handler(func=lambda call: call.data == "back_to_admin_panel")
def handle_back_to_admin_panel(call):
    """
    Обрабатывает все кнопки "Назад" в админке.
    """
    if not is_admin(call.from_user.id): return
    try:
        bot.answer_callback_query(call.id)
        # Редактируем сообщение, превращая его в главное меню
        show_admin_panel(call.message.chat.id, call.from_user.id, message_id=call.message.message_id)
    except Exception as e:
        print(f"Ошибка handle_back_to_admin_panel: {e}")
    
    # Очищаем состояние пользователя, если он вышел из пошагового процесса
    if call.from_user.id in user_state:
        del user_state[call.from_user.id]
# ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.3 ▲▲▲

@bot.callback_query_handler(func=lambda call: call.data == "admin_user_stats")
def show_user_stats(call):
    if not is_admin(call.from_user.id):
        try: bot.answer_callback_query(call.id, "❌ Доступ запрещён!", show_alert=True)
        except Exception: pass
        return
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    conn = get_db_conn()
    if not conn:
        bot.send_message(call.message.chat.id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT user_id, username, first_name, last_name, first_visit, last_visit, total_commands 
                                FROM user_activity ORDER BY last_visit DESC''')
                rows = cur.fetchall()
        text = "👥 СТАТИСТИКА ПОЛЬЗОВАТЕЛЕЙ БОТА\n" + "=" * 50 + "\n\n"
        if not rows:
            text += "❌ Нет активности\n"
        else:
            text += f"📊 Всего уникальных пользователей: {len(rows)}\n\n"
            for idx, (user_id, username, first_name, last_name, first_visit, last_visit, total_commands) in enumerate(rows, 1):
                full_name = f"{first_name} {last_name}".strip()
                username_str = f"@{username}" if username else "нет username"
                text += f"{idx}. {full_name} ({username_str})\n"
                text += f"    ID: {user_id}\n"
                text += f"    Первый визит: {first_visit}\n"
                text += f"    Последний визит: {last_visit}\n"
                text += f"    Команд использовано: {total_commands}\n\n"
        bot.send_message(call.message.chat.id, text)
    except Exception as e:
        print(f"Ошибка show_user_stats: {e}")
        bot.send_message(call.message.chat.id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)

# =========================================================================
# ========== ИСПРАВЛЕННЫЙ БЛОК: УПРАВЛЕНИЕ СОЕДИНЕНИЯМИ ==========
# =========================================================================

def show_player_list_for_action(chat_id, user_id, action_prefix, text_prompt):
    """
    Показывает список игроков.
    Эта функция теперь использует КЭШ и НЕ использует БД.
    """
    try:
        # --- ИСПОЛЬЗУЕМ КЭШ ---
        players_from_cache = player_cache.get_players()
        # ---------------------
        
        if not players_from_cache:
            bot.send_message(chat_id, "❌ Нет игроков в системе. Сначала добавьте игроков.")
            return
        
        markup = types.InlineKeyboardMarkup()
        row = []
        for player in players_from_cache:
            row.append(types.InlineKeyboardButton(f"👤 {player['nickname']}", callback_data=f"{action_prefix}_{player['nickname']}"))
            if len(row) == 2:
                markup.add(*row)
                row = []
        if row: markup.add(*row)
        
        # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
        # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
        bot.send_message(chat_id, text_prompt, reply_markup=markup)
    
    except Exception as e:
        print(f"Ошибка в show_player_list_for_action: {e}")
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    
    # 'finally' и 'conn' больше не нужны

# ▼▼▼ ИСПРАВЛЕНИЕ 3.5: УДАЛЕНИЕ СТАРОЙ ФУНКЦИИ ОТМЕНЫ ▼▼▼
# @bot.callback_query_handler(func=lambda call: call.data == "cancel_admin_action")
# ... (ФУНКЦИЯ УДАЛЕНА) ...
# ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.5 ▲▲▲

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def admin_buttons(call):
    """
    Эта функция теперь РАЗДЕЛЕНА:
    1. Telegram-операции (БЕЗ БД).
    2. Операции, которые ВЫЗЫВАЮТ ДРУГИЕ ФУНКЦИИ (им не нужен conn).
    3. Операции, которым НУЖНА БД (в блоке try-finally).
    """
    if not is_admin(call.from_user.id):
        try: bot.answer_callback_query(call.id, "❌ Доступ запрещён!", show_alert=True)
        except Exception: pass
        return
    
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    
    # --- 1. Telegram-операции (БЕЗ БД) ---
    try:
        bot.answer_callback_query(call.id)
        # Редактируем сообщение в show_admin_panel, если это "Назад"
        if call.data != "back_to_admin_panel":
             bot.delete_message(chat_id, call.message.message_id)
    except Exception: 
        pass
    
    # --- 2. Операции, которые ВЫЗЫВАЮТ ДРУГИЕ ФУНКЦИИ (им не нужен conn) ---
    # Эти функции теперь берут данные из КЭША
    if call.data == "admin_manage_roles":
        show_player_list_for_action(chat_id, user_id, "select_for_manage_roles", "📊 Выберите игрока для управления ролями:")
        return
        
    if call.data == "admin_set_rating":
        show_player_list_for_action(chat_id, user_id, "select_for_set_rating", "📝 Выберите игрока для изменения рейтинга:")
        return
        
    if call.data == "admin_add_mmr":
        show_player_list_for_action(chat_id, user_id, "select_for_add_mmr", "🎖️ Выберите игрока для установки MMR:")
        return
        
    if call.data == "admin_set_positions":
        show_player_list_for_action(chat_id, user_id, "select_for_set_positions", "🎯 Выберите игрока для установки позиций:")
        return
        
    if call.data == "admin_delete_player":
        show_player_list_for_action(chat_id, user_id, "select_for_delete_player", "🗑️ Выберите игрока для УДАЛЕНИЯ:")
        return

    # --- 3. Операции, которым НУЖНА БД или STATE (в try-finally) ---
    
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    
    try:
        with conn:
            with conn.cursor() as cur:
                if call.data == "admin_add_player":
                    user_state[user_id] = {"action": "waiting_add_player"}
                    bot.send_message(chat_id, "Введите nickname игрока:\nПример: PlayerName")
                
                elif call.data == "admin_add_game":
                    user_state[user_id] = {"action": "waiting_add_game_screenshot"}
                    bot.send_message(chat_id, "Отправьте скриншот игры:")
                
                elif call.data == "admin_undo_game":
                    cur.execute("SELECT id, radiant_players, dire_players, result, date FROM games ORDER BY id DESC LIMIT 1")
                    last_game = cur.fetchone()
                    if not last_game:
                        bot.send_message(chat_id, "❌ Нет игр в базе данных для отмены.")
                        return
                    game_id, radiant, dire, result, date = last_game
                    text = (f"⚠️ Вы уверены, что хотите отменить последнюю игру?\n\n"
                            f"<b>ID Игры:</b> {game_id}\n"
                            f"<b>Дата:</b> {date}\n"
                            f"<b>Команды:</b> {radiant} (🟢) vs {dire} (🔴)\n"
                            f"<b>Победитель:</b> {result.upper()}\n\n"
                            f"Это действие необратимо и откатит весь рейтинг, KDA и W/L для 10 игроков.")
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("✅ Да, отменить эту игру", callback_data=f"confirm_undo_{game_id}"))
                    # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
                    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
                    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
                    bot.send_message(chat_id, text, reply_markup=markup)
                
                elif call.data == "admin_add_hero":
                    user_state[user_id] = {"action": "waiting_add_hero_input"}
                    text = "⚔️ <b>ДОБАВЛЕНИЕ ГЕРОЯ ИГРОКУ</b>\n\n"
                    text += "Формат: <code>nickname hero position wins losses kills deaths assists</code>\n\n"
                    text += "Позиции: 1=Carry, 2=Mid, 3=Offlane, 4=SoftSupport, 5=HardSupport\n\n"
                    text += "Пример: <code>law Anti-Mage 1 5 3 45 12 67</code>"
                    bot.send_message(chat_id, text)
                
                elif call.data == "admin_list":
                    cur.execute('SELECT nickname, rating, wins, losses, mmr, positions FROM players ORDER BY rating DESC')
                    rows = cur.fetchall()
                    if not rows:
                        bot.send_message(chat_id, "📭 Нет игроков в базе.")
                    else:
                        text = "📋 Список всех игроков:\n\n"
                        for idx, (nickname, rating, wins, losses, mmr, positions_json) in enumerate(rows, 1):
                            total = wins + losses
                            wr = round((wins / total * 100), 1) if total > 0 else 0
                            try:
                                positions_list = json.loads(positions_json) if positions_json else []
                            except Exception:
                                positions_list = []
                            pos_str = get_player_positions_str(positions_list)
                            text += f"{idx}. {nickname}\n"
                            text += f"    Рейтинг: {rating} | W/L: {wins}/{losses} | WR: {wr}% | MMR: {mmr}\n"
                            text += f"    Позиции: {pos_str}\n\n"
                        bot.send_message(chat_id, text)
    
    except Exception as e:
        print(f"Ошибка в admin_buttons: {e}")
        try: 
            bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
        except Exception: 
            pass
    
    finally:
        put_db_conn(conn)

# =========================================================================
# ========== КОНЕЦ ИСПРАВЛЕННОГО БЛОКА: УПРАВЛЕНИЕ СОЕДИНЕНИЯМИ ==========
# =========================================================================


@bot.message_handler(
    func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_add_player")
def handle_add_player(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    nickname = message.text.strip()
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'INSERT INTO players (nickname, rating, wins, losses, mmr, positions, total_kills, total_deaths, total_assists) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (nickname) DO NOTHING',
                    (nickname, 1000, 0, 0, 0, '[]', 0, 0, 0)
                )
        bot.send_message(chat_id, f"✅ Игрок {nickname} добавлен с начальным рейтингом 1000")
        del user_state[user_id]
        player_cache.invalidate() # <-- ОБНОВЛЯЕМ КЭШ
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
        if user_id in user_state: del user_state[user_id]
    finally:
        put_db_conn(conn)


@bot.message_handler(
    func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_add_hero_input")
def handle_add_hero_input(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    nickname = "" 
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                tokens = text.rsplit(maxsplit=6)
                # Разбиваем строку только на ник и остальную часть, чтобы имя героя могло содержать пробелы
                parts = text.split(maxsplit=1)
                if len(parts) < 2:
                    bot.send_message(chat_id, "❌ Неверный формат. Используйте:\n<code>nickname hero position wins losses kills deaths assists</code>")
                    return
                nickname, rest = parts[0], parts[1].strip()
                # Отделяем имя героя слева и 6 числовых показателей справа
                hero_and_stats = rest.rsplit(maxsplit=6)
                if len(hero_and_stats) != 7:
                    bot.send_message(chat_id, "❌ Неверный формат. Используйте:\n<code>nickname hero position wins losses kills deaths assists</code>")
                    return
                hero_name = hero_and_stats[0].strip()
                if not hero_name:
                    bot.send_message(chat_id, "❌ Укажите название героя после ника игрока.")
                    return
                try:
                    position, wins, losses, kills, deaths, assists = map(int, hero_and_stats[1:])
                except ValueError:
                    bot.send_message(chat_id, "❌ Ошибка в числовых значениях. Используйте числа для позиции и статистики.")
                    return
                if position not in POSITIONS:
                    bot.send_message(chat_id, f"❌ Неверная позиция {position}! Используйте 1-5.")
                    return
                if any(value < 0 for value in (wins, losses, kills, deaths, assists)):
                    bot.send_message(chat_id, "❌ Статистика не может содержать отрицательные значения.")
                    return
                cur.execute('SELECT nickname FROM players WHERE nickname=%s', (nickname,))
                if not cur.fetchone():
                    bot.send_message(chat_id, f"❌ Игрок '{nickname}' не найден!")
                    del user_state[user_id]
                    return
                cur.execute('''
                    INSERT INTO player_heroes (player_nickname, hero_name, wins, losses, total_kills, total_deaths, total_assists)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(player_nickname, hero_name) DO UPDATE SET
                        wins = player_heroes.wins + %s,
                        losses = player_heroes.losses + %s,
                        total_kills = player_heroes.total_kills + %s,
                        total_deaths = player_heroes.total_deaths + %s,
                        total_assists = player_heroes.total_assists + %s
                ''', (nickname, hero_name, wins, losses, kills, deaths, assists,
                      wins, losses, kills, deaths, assists))
                cur.execute('''
                    INSERT INTO player_role_stats (player_nickname, role_position, wins, losses)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(player_nickname, role_position) DO UPDATE SET
                        wins = player_role_stats.wins + %s,
                        losses = player_role_stats.losses + %s
                ''', (nickname, position, wins, losses, wins, losses))
                rating_change = (wins * RATING_CHANGE) - (losses * RATING_CHANGE)
                cur.execute(
                    '''UPDATE players SET 
                            wins = wins + %s, losses = losses + %s, rating = rating + %s, 
                            total_kills = total_kills + %s, total_deaths = total_deaths + %s, 
                            total_assists = total_assists + %s
                        WHERE nickname=%s''',
                    (wins, losses, rating_change, kills, deaths, assists, nickname)
                )
        print("✅✅✅ ТРАНЗАКЦИЯ УСПЕШНО ЗАВЕРШЕНА (COMMIT)")
        position_name = POSITIONS.get(position, "?")
        total_games = wins + losses
        wr = round((wins / total_games * 100), 1) if total_games > 0 else 0
        if deaths > 0:
            kda_value = round((kills + assists) / deaths, 2)
            kda_str = f"{kda_value:.2f}"
        else:
            kda_str = "∞" if (kills + assists) > 0 else "0"
        success_text = f"✅ <b>ГЕРОЙ ДОБАВЛЕН УСПЕШНО!</b>\n\n"
        success_text += f"👤 Игрок: <b>{nickname}</b>\n"
        success_text += f"⚔️ Герой: <b>{hero_name}</b>\n"
        success_text += f"🎯 Позиция: <b>{position_name}</b>\n\n"
        success_text += f"📊 W/L: <b>{wins}/{losses}</b> | WR: <b>{wr}%</b>\n"
        success_text += f"📊 KDA: <b>{kills}/{deaths}/{assists}</b> = {kda_str}\n\n"
        success_text += f"💰 Рейтинг: <b>{rating_change:+d}</b> | Роль: <b>+{wins}W +{losses}L</b>"
        bot.send_message(chat_id, success_text)
        player_cache.invalidate() # <-- ОБНОВЛЯЕМ КЭШ
    except Exception as e:
        print(f"❌❌❌ ОШИБКА ТРАНЗАКЦИИ: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(chat_id, f"❌ КРИТИЧЕСКАЯ ОШИБКА:\n<code>{str(e)}</code>")
    finally:
        put_db_conn(conn)
        if user_id in user_state:
            del user_state[user_id]
            print(f"Состояние для {user_id} очищено.")


# =========================================================================
# ========== БЛОК "УПРАВЛЕНИЯ РОЛЯМИ" (ЦИКЛИЧЕСКИЙ) ==========
# =========================================================================

def show_role_management_menu(user_id, chat_id, nickname, message_id=None, prefix_text=""):
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    'SELECT role_position, wins, losses FROM player_role_stats WHERE player_nickname=%s ORDER BY role_position',
                    (nickname,))
                roles = cur.fetchall()
        user_state[user_id] = {"action": "waiting_manage_roles_action", "nickname": nickname, "roles": roles}
        if not roles:
            text = f"{prefix_text}❌ У {nickname} нет статистики по ролям. Вы можете добавить их."
        else:
            text = f"{prefix_text}📊 Роли {nickname}:\n\n"
            for role_pos, wins, losses in roles:
                total = wins + losses
                wr = round((wins / total * 100), 1) if total > 0 else 0
                role_name = POSITIONS.get(role_pos, "?")
                text += f"{role_pos}. {role_name}: W/L {wins}/{losses} | WR {wr}%\n"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("➕ Добавить роль", callback_data=f"add_role_for_{nickname}"))
        markup.add(types.InlineKeyboardButton("✏️ Редактировать роль", callback_data=f"edit_role_{nickname}"))
        markup.add(types.InlineKeyboardButton("🗑️ Удалить роль", callback_data=f"delete_role_{nickname}"))
        # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
        markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
        # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        else:
            bot.send_message(chat_id, text, reply_markup=markup)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка в show_role_management_menu: {str(e)}")
        if user_id in user_state: del user_state[user_id]
    finally:
        put_db_conn(conn)

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_for_manage_roles_"))
def handle_select_player_for_manage_roles(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    nickname = call.data.replace("select_for_manage_roles_", "")
    try:
        bot.delete_message(chat_id, call.message.message_id)
        bot.answer_callback_query(call.id, f"Выбран: {nickname}")
    except Exception: pass
    show_role_management_menu(user_id, chat_id, nickname)

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_role_for_"))
def handle_add_role_start(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    nickname = state["nickname"]
    existing_roles = [role_data[0] for role_data in state["roles"]]
    missing_roles = []
    for pos_id, pos_name in POSITIONS.items():
        if pos_id not in existing_roles:
            missing_roles.append((pos_id, pos_name))
    if not missing_roles:
        bot.answer_callback_query(call.id, "✅ У игрока уже есть все 5 ролей.", show_alert=True)
        return
    user_state[user_id]["action"] = "waiting_select_role_to_add"
    markup = types.InlineKeyboardMarkup()
    for pos_id, pos_name in missing_roles:
        markup.add(types.InlineKeyboardButton(f"➕ {pos_id}. {pos_name}", callback_data=f"confirm_add_role_{pos_id}"))
    markup.add(types.InlineKeyboardButton("❌ Назад", callback_data="back_to_role_menu"))
    bot.edit_message_text(f"Выберите роль для добавления (0/0) игроку {nickname}:", chat_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_add_role_"))
def handle_add_role_confirm(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                role_pos = int(call.data.replace("confirm_add_role_", ""))
                nickname = user_state[user_id]["nickname"]
                cur.execute('''
                    INSERT INTO player_role_stats (player_nickname, role_position, wins, losses)
                    VALUES (%s, %s, 0, 0)
                    ON CONFLICT(player_nickname, role_position) DO NOTHING
                ''', (nickname, role_pos))
        role_name = POSITIONS.get(role_pos, "?")
        prefix_text = f"✅ Роль {role_name} (0/0) добавлена!\n\n"
        # ▼▼▼ ИСПРАВЛЕНИЕ 4: ДОБАВЛЕН СБРОС КЭША ▼▼▼
        player_cache.invalidate()
        # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 4 ▲▲▲
        show_role_management_menu(user_id, chat_id, nickname, message_id=call.message.message_id, prefix_text=prefix_text)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при добавлении роли: {str(e)}")
    finally:
        put_db_conn(conn)

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_role_"))
def edit_role(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    roles = state["roles"]
    if not roles:
        bot.answer_callback_query(call.id, "❌ У игрока нет ролей для редактирования.", show_alert=True)
        return
    user_state[user_id]["action"] = "waiting_select_role_to_edit"
    markup = types.InlineKeyboardMarkup()
    for role_pos, wins, losses in roles:
        role_name = POSITIONS.get(role_pos, "?")
        markup.add(
            types.InlineKeyboardButton(
                f"{role_pos}. {role_name} ({wins}W-{losses}L)",
                callback_data=f"select_edit_role_{role_pos}"
            )
        )
    markup.add(types.InlineKeyboardButton("❌ Назад", callback_data="back_to_role_menu"))
    bot.edit_message_text("Выберите роль для редактирования:", chat_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_role_menu")
def back_to_role_menu(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    nickname = user_state[user_id].get("nickname")
    if not nickname: return
    show_role_management_menu(user_id, chat_id, nickname, message_id=call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_edit_role_"))
def select_edit_role(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    try:
        role_pos = int(call.data.replace("select_edit_role_", ""))
        state = user_state[user_id]
        user_state[user_id] = {
            "action": "waiting_edit_role_stats",
            "nickname": state["nickname"],
            "role_position": role_pos,
            "roles": state["roles"],
            "message_id": call.message.message_id
        }
        bot.edit_message_text(
            f"Введите новые значения для роли {POSITIONS.get(role_pos, '?')} (игрок {state['nickname']}):\n\n"
            f"Формат: wins losses\nПример: 10 5",
            chat_id,
            call.message.message_id
        )
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")

@bot.message_handler(
    func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_edit_role_stats")
def handle_edit_role_stats(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                parts = message.text.strip().split()
                if len(parts) != 2:
                    bot.send_message(chat_id, "❌ Неверный формат. Используйте: wins losses\nПример: 10 5")
                    return
                wins = int(parts[0])
                losses = int(parts[1])
                state = user_state[user_id]
                nickname = state["nickname"]
                role_position = state["role_position"]
                message_id = state.get("message_id")
                cur.execute(
                    '''UPDATE player_role_stats SET wins=%s, losses=%s WHERE player_nickname=%s AND role_position=%s''',
                    (wins, losses, nickname, role_position)
                )
        try:
            if message_id:
                bot.delete_message(chat_id, message_id)
        except Exception: pass
        try:
            bot.delete_message(chat_id, message.message_id)
        except Exception: pass
        role_name = POSITIONS.get(role_position, "?")
        prefix_text = f"✅ Статистика {role_name} обновлена!\n\n"
        # ▼▼▼ ИСПРАВЛЕНИЕ 4: ДОБАВЛЕН СБРОС КЭША ▼▼▼
        player_cache.invalidate()
        # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 4 ▲▲▲
        show_role_management_menu(user_id, chat_id, nickname, prefix_text=prefix_text)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
        if user_id in user_state: del user_state[user_id]
    finally:
        put_db_conn(conn)

@bot.callback_query_handler(func=lambda call: call.data.startswith("delete_role_"))
def delete_role(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    roles = state["roles"]
    if not roles:
        bot.answer_callback_query(call.id, "❌ У игрока нет ролей для удаления.", show_alert=True)
        return
    user_state[user_id]["action"] = "waiting_select_role_to_delete"
    markup = types.InlineKeyboardMarkup()
    for role_pos, wins, losses in roles:
        role_name = POSITIONS.get(role_pos, "?")
        markup.add(
            types.InlineKeyboardButton(
                f"🗑️ {role_pos}. {role_name} ({wins}W-{losses}L)",
                callback_data=f"confirm_delete_role_{role_pos}"
            )
        )
    markup.add(types.InlineKeyboardButton("❌ Назад", callback_data="back_to_role_menu"))
    bot.edit_message_text("Выберите роль для удаления:", chat_id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete_role_"))
def confirm_delete_role(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                role_pos = int(call.data.replace("confirm_delete_role_", ""))
                state = user_state[user_id]
                nickname = state["nickname"]
                cur.execute('DELETE FROM player_role_stats WHERE player_nickname=%s AND role_position=%s',
                            (nickname, role_pos))
        role_name = POSITIONS.get(role_pos, "?")
        prefix_text = f"✅ Роль {role_name} удалена!\n\n"
        # ▼▼▼ ИСПРАВЛЕНИЕ 4: ДОБАВЛЕН СБРОС КЭША ▼▼▼
        player_cache.invalidate()
        # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 4 ▲▲▲
        show_role_management_menu(user_id, chat_id, nickname, message_id=call.message.message_id, prefix_text=prefix_text)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)

# ▼▼▼ ИСПРАВЛЕНИЕ 3.5: УДАЛЕНИЕ СТАРОЙ ФУНКЦИИ ОТМЕНЫ ▼▼▼
# @bot.callback_query_handler(func=lambda call: call.data == "cancel_manage_roles")
# ... (ФУНКЦИЯ УДАЛЕНА) ...
# ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.5 ▲▲▲


# ... (Остальные обработчики `handle_add_game_screenshot` и т.д.) ...

@bot.message_handler(
    content_types=['photo'],
    func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_add_game_screenshot"
)
def handle_add_game_screenshot(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                photo = message.photo[-1]
                screenshot_file_id = photo.file_id
                cur.execute('SELECT nickname FROM players ORDER BY nickname')
                players = [row[0] for row in cur.fetchall()]
        if not players:
            bot.send_message(chat_id, "❌ Нет игроков в системе. Сначала добавьте игроков.")
            del user_state[user_id]
            return
        user_state[user_id] = {
            "action": "waiting_radiant_players", "screenshot_file_id": screenshot_file_id,
            "players": players, "radiant_selected": [], "dire_selected": [], "player_stats": {}
        }
        markup = types.InlineKeyboardMarkup()
        for player in players:
            markup.add(types.InlineKeyboardButton(f"{player}", callback_data=f"select_radiant_{player}"))
        markup.add(types.InlineKeyboardButton("✅ Готово с Radiant", callback_data="done_radiant"))
        bot.send_message(chat_id, "🟢 Выберите игроков за RADIANT:", reply_markup=markup)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
        if user_id in user_state: del user_state[user_id]
    finally:
        put_db_conn(conn)


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_radiant_"))
def select_radiant_player(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    player = call.data.replace("select_radiant_", "")
    if player not in state["radiant_selected"]:
        state["radiant_selected"].append(player)
    markup = types.InlineKeyboardMarkup()
    for p in state["players"]:
        if p in state["radiant_selected"]:
            markup.add(types.InlineKeyboardButton(f"✅ {p}", callback_data=f"remove_radiant_{p}"))
        else:
            markup.add(types.InlineKeyboardButton(f"{p}", callback_data=f"select_radiant_{p}"))
    markup.add(types.InlineKeyboardButton("✅ Готово с Radiant", callback_data="done_radiant"))
    try: bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_radiant_"))
def remove_radiant_player(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    player = call.data.replace("remove_radiant_", "")
    if player in state["radiant_selected"]:
        state["radiant_selected"].remove(player)
    markup = types.InlineKeyboardMarkup()
    for p in state["players"]:
        if p in state["radiant_selected"]:
            markup.add(types.InlineKeyboardButton(f"✅ {p}", callback_data=f"remove_radiant_{p}"))
        else:
            markup.add(types.InlineKeyboardButton(f"{p}", callback_data=f"select_radiant_{p}"))
    markup.add(types.InlineKeyboardButton("✅ Готово с Radiant", callback_data="done_radiant"))
    try: bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data == "done_radiant")
def done_radiant(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    state["action"] = "waiting_dire_players"
    markup = types.InlineKeyboardMarkup()
    for player in state["players"]:
        if player not in state["radiant_selected"]:
            markup.add(types.InlineKeyboardButton(f"{player}", callback_data=f"select_dire_{player}"))
    markup.add(types.InlineKeyboardButton("✅ Готово с Dire", callback_data="done_dire"))
    try: bot.edit_message_text("🔴 Выберите игроков за DIRE:", chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("select_dire_"))
def select_dire_player(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    player = call.data.replace("select_dire_", "")
    if player not in state["dire_selected"]:
        state["dire_selected"].append(player)
    markup = types.InlineKeyboardMarkup()
    for p in state["players"]:
        if p in state["radiant_selected"]: continue
        if p in state["dire_selected"]:
            markup.add(types.InlineKeyboardButton(f"✅ {p}", callback_data=f"remove_dire_{p}"))
        else:
            markup.add(types.InlineKeyboardButton(f"{p}", callback_data=f"select_dire_{p}"))
    markup.add(types.InlineKeyboardButton("✅ Готово с Dire", callback_data="done_dire"))
    try: bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_dire_"))
def remove_dire_player(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    player = call.data.replace("remove_dire_", "")
    if player in state["dire_selected"]:
        state["dire_selected"].remove(player)
    markup = types.InlineKeyboardMarkup()
    for p in state["players"]:
        if p in state["radiant_selected"]: continue
        if p in state["dire_selected"]:
            markup.add(types.InlineKeyboardButton(f"✅ {p}", callback_data=f"remove_dire_{p}"))
        else:
            markup.add(types.InlineKeyboardButton(f"{p}", callback_data=f"select_dire_{p}"))
    markup.add(types.InlineKeyboardButton("✅ Готово с Dire", callback_data="done_dire"))
    try: bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data == "done_dire")
def done_dire(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    if state["radiant_selected"]:
        state["action"] = "entering_player_stats"
        state["current_team"] = "radiant"
        state["current_player_index"] = 0
        current_player = state["radiant_selected"][0]
        state["current_player"] = current_player
        bot.send_message(
            chat_id,
            (
                f"🟢 Введите данные для {current_player} (Radiant)\n\n"
                "Формат: Герой Убийства Смерти Ассисты\nПример: Anti-Mage 10 3 15"
            )
        )
    else:
        show_result_selection(chat_id, state)


@bot.message_handler(
    func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "entering_player_stats")
def handle_player_stats(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    if user_id not in user_state: return
    state = user_state[user_id]
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                parts = message.text.strip().split()
                if len(parts) < 4:
                    bot.send_message(chat_id, "❌ Неверный формат...")
                    return
                hero = " ".join(parts[:-3])
                kills = int(parts[-3])
                deaths = int(parts[-2])
                assists = int(parts[-1])
                current_player = state["current_player"]
                state["temp_stats"] = {
                    "hero": hero, "kills": kills, "deaths": deaths,
                    "assists": assists, "team": state["current_team"]
                }
                state["action"] = "selecting_player_role"
                
                # ▼▼▼ ИСПРАВЛЕНИЕ 2: ЧИНИМ ЛОГИКУ ВЫБОРА РОЛЕЙ ▼▼▼
                cur.execute('SELECT role_position FROM player_role_stats WHERE player_nickname=%s ORDER BY role_position', (current_player,))
                rows = cur.fetchall()
                player_positions_from_stats = [row[0] for row in rows]

                # Если у игрока нет ВООБЩЕ никаких ролей, показываем все 5
                if not player_positions_from_stats:
                    player_positions_to_show = list(POSITIONS.keys())
                else:
                    player_positions_to_show = player_positions_from_stats
                # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 2 ▲▲▲
        
        # Этот try/except блок был ошибочно внутри with conn:
        try:
            markup = types.InlineKeyboardMarkup()
            for pos in player_positions_to_show: # <--- Используем новую переменную
                pos_name = POSITIONS.get(pos, "Неизвестная")
                markup.add(types.InlineKeyboardButton(f"{pos}. {pos_name}", callback_data=f"set_game_role_{pos}"))
            bot.send_message(chat_id, f"Выберите роль для {current_player} в этой игре:", reply_markup=markup)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Ошибка: {str(e)}...")
            
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}...")
    finally:
        put_db_conn(conn)


@bot.callback_query_handler(func=lambda call: call.data.startswith("set_game_role_"))
def handle_game_role_selection(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    if state.get("action") != "selecting_player_role": return
    try:
        role_position = int(call.data.replace("set_game_role_", ""))
        current_player = state["current_player"]
        temp_stats = state["temp_stats"]
        state["player_stats"][current_player] = {
            "hero": temp_stats["hero"], "kills": temp_stats["kills"], "deaths": temp_stats["deaths"],
            "assists": temp_stats["assists"], "team": temp_stats["team"], "position": role_position
        }
        del state["temp_stats"]
        state["action"] = "entering_player_stats"
        state["current_player_index"] += 1
        if state["current_team"] == "radiant":
            if state["current_player_index"] < len(state["radiant_selected"]):
                current_player = state["radiant_selected"][state["current_player_index"]]
                state["current_player"] = current_player
                message_text = (
                    f"✅ Роль выбрана!\n\n🟢 Введите данные для {current_player} (Radiant)\n\n"
                    "Формат: Герой Убийства Смерти Ассисты"
                )
                bot.edit_message_text(message_text, chat_id=chat_id, message_id=call.message.message_id),
                    chat_id=chat_id,
                    message_id=call.message.message_id
                )
            else:
                state["current_team"] = "dire"
                state["current_player_index"] = 0
                if state["dire_selected"]:
                    current_player = state["dire_selected"][0]
                    state["current_player"] = current_player
                    message_text = (
                        f"✅ Роль выбрана!\n\n🔴 Введите данные для {current_player} (Dire)\n\n"
                        "Формат: Герой Убийства Смерти Ассисты"
                    )
                    bot.edit_message_text(message_text, chat_id=chat_id, message_id=call.message.message_id)
                else:
                    bot.edit_message_text("✅ Роль выбрана!", chat_id=chat_id, message_id=call.message.message_id)
                    show_result_selection(chat_id, state)
        else:
            if state["current_player_index"] < len(state["dire_selected"]):
                current_player = state["dire_selected"][state["current_player_index"]]
                state["current_player"] = current_player
                message_text = (
                    f"✅ Роль выбрана!\n\n🔴 Введите данные для {current_player} (Dire)\n\n"
                    "Формат: Герой Убийства Смерти Ассисты"
                )
                bot.edit_message_text(message_text, chat_id=chat_id, message_id=call.message.message_id)
            else:
                bot.edit_message_text("✅ Роль выбрана!", chat_id=chat_id, message_id=call.message.message_id)
                show_result_selection(chat_id, state)
        else:
            if state["current_player_index"] < len(state["dire_selected"]):
                current_player = state["dire_selected"][state["current_player_index"]]
                state["current_player"] = current_player
                bot.edit_message_text(
                    f"✅ Роль выбрана!\n\n🔴 Введите данные для {current_player} (Dire)\n\n"
                    f"Формат: Герой Убийства Смерти Ассисты",
                    chat_id=chat_id,
                    message_id=call.message.message_id
                )
            else:
                bot.edit_message_text(
                    "✅ Роль выбрана!",
                    chat_id=chat_id,
                    message_id=call.message.message_id
                )
                show_result_selection(chat_id, state)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")


def show_result_selection(chat_id, state):
    state["action"] = "waiting_game_result"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🟢 Radiant WIN", callback_data="result_radiant"))
    markup.add(types.InlineKeyboardButton("🔴 Dire WIN", callback_data="result_dire"))
    try:
        bot.send_message(chat_id, "Кто победил?", reply_markup=markup)
    except Exception: pass


@bot.callback_query_handler(func=lambda call: call.data in ["result_radiant", "result_dire"])
def set_game_result(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    if user_id not in user_state: return
    state = user_state[user_id]
    result = "radiant" if call.data == "result_radiant" else "dire"
    winners = state["radiant_selected"] if result == "radiant" else state["dire_selected"]
    radiant_str = ", ".join(state["radiant_selected"])
    dire_str = ", ".join(state["dire_selected"])
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                now = datetime.now()
                date_str = now.strftime("%Y-%m-%d")
                time_str = now.strftime("%H:%M")
                cur.execute('''INSERT INTO games 
                                (screenshot_file_id, radiant_players, dire_players, result, date, time, description) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id''',
                            (state['screenshot_file_id'], radiant_str, dire_str, result, date_str, time_str, ""))
                game_id = cur.fetchone()[0]
                text_report = f"✅ Игра {game_id} добавлена!\n🏆 Победители: {result.upper()}\n\n"
                team_reports = {"radiant": "🟢 RADIANT:\n", "dire": "🔴 DIRE:\n"}
                for player, stats in state["player_stats"].items():
                    hero_name = stats["hero"]
                    kills = stats["kills"]
                    deaths = stats["deaths"]
                    assists = stats["assists"]
                    position = stats.get("position", 0)
                    team = stats["team"]
                    cur.execute('''INSERT INTO player_game_stats 
                                    (game_id, player_nickname, hero, kills, deaths, assists, team, position) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
                                (game_id, player, hero_name, kills, deaths, assists, team, position))
                    cur.execute(
                        'SELECT wins, losses, rating, total_kills, total_deaths, total_assists '
                        'FROM players WHERE nickname=%s FOR UPDATE', (player,))
                    row = cur.fetchone()
                    if row:
                        wins, losses, rating, total_kills, total_deaths, total_assists = row
                        is_winner = (player in winners)
                        rating_change_val = 0 
                        if is_winner:
                            wins += 1
                            rating += RATING_CHANGE
                            rating_change_val = RATING_CHANGE
                            rating_change_str = f"+{rating_change_val}"
                        else:
                            losses += 1
                            rating_before_loss = rating
                            rating = max(0, rating - RATING_CHANGE)
                            rating_change_val = rating - rating_before_loss 
                            rating_change_str = f"{rating_change_val}"
                        total_kills += kills
                        total_deaths += deaths
                        total_assists += assists
                        cur.execute(
                            'UPDATE players SET wins=%s, losses=%s, rating=%s, total_kills=%s, total_deaths=%s, total_assists=%s '
                            'WHERE nickname=%s',
                            (wins, losses, rating, total_kills, total_deaths, total_assists, player)
                        )
                        new_wr = round((wins / (wins + losses) * 100), 1) if (wins + losses) > 0 else 0
                        role_name = POSITIONS.get(position, "Не указано")
                        team_reports[team] += (f"    {player} ({hero_name}) - Роль: {role_name}\n"
                                               f"    KDA: {kills}/{deaths}/{assists}\n"
                                               f"    Рейтинг: {rating} ({rating_change_str}) | WR: {new_wr}%\n\n")
                        is_win = 1 if is_winner else 0
                        cur.execute('''
                            INSERT INTO player_heroes (player_nickname, hero_name, wins, losses, total_kills, total_deaths, total_assists)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(player_nickname, hero_name) DO UPDATE SET
                                wins = player_heroes.wins + %s,
                                losses = player_heroes.losses + %s,
                                total_kills = player_heroes.total_kills + %s,
                                total_deaths = player_heroes.total_deaths + %s,
                                total_assists = player_heroes.total_assists + %s
                        ''', (player, hero_name, is_win, 1 - is_win, kills, deaths, assists,
                              is_win, 1 - is_win, kills, deaths, assists))
                        if position > 0:
                            cur.execute('''
                                INSERT INTO player_role_stats (player_nickname, role_position, wins, losses)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT(player_nickname, role_position) DO UPDATE SET
                                    wins = player_role_stats.wins + %s,
                                    losses = player_role_stats.losses + %s
                            ''', (player, position, is_win, 1 - is_win, is_win, 1 - is_win))
        
        text_report += team_reports["radiant"] + team_reports["dire"]
        bot.send_message(chat_id, text_report)
        player_cache.invalidate() # <-- ОБНОВЛЯЕМ КЭШ ПОСЛЕ ИГРЫ
    except Exception as e:
        print(f"Ошибка set_game_result: {e}")
        bot.send_message(chat_id, f"❌ Ошибка при сохранении игры: {str(e)}")
    finally:
        put_db_conn(conn)
        if user_id in user_state:
            del user_state[user_id]

# ▼▼▼ НОВЫЕ ФУНКЦИИ ДЛЯ /statshero ▼▼▼
def get_global_hero_stats_text(min_games=2):
    """
    Собирает статистику по топ-10 героям лиги и ищет лучших игроков.
    min_games - порог для определения "Лучшего игрока".
    """
    conn = get_db_conn()
    if not conn: return "Ошибка подключения к БД."
    
    top_heroes = []
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Получаем топ 10 популярных героев (здесь ничего не меняем)
                cur.execute('''
                    SELECT 
                        hero_name,
                        SUM(wins + losses) AS total_games,
                        (SUM(CAST(wins AS FLOAT)) / SUM(wins + losses)) * 100 AS winrate,
                        (CASE 
                            WHEN SUM(total_deaths) = 0 THEN (SUM(total_kills + total_assists))
                            ELSE (SUM(CAST(total_kills AS FLOAT) + total_assists)) / SUM(total_deaths)
                        END) AS kda
                    FROM player_heroes
                    WHERE (wins + losses) > 0
                    GROUP BY hero_name
                    ORDER BY total_games DESC
                    LIMIT 10
                ''')
                top_heroes = cur.fetchall()
                
                if not top_heroes:
                    return "🦸 Статистика по героям пока пуста. Сыграйте больше игр!"
                    
                text = "🦸 <b>ТОП-10 ГЕРОЕВ ЛИГИ</b> 🦸\n" + "=" * 50 + "\n\n"
                
                for idx, (hero, games, wr, kda) in enumerate(top_heroes, 1):
                    # 2. Для каждого героя ищем лучшего игрока
                    
                    # ▼▼▼ ИЗМЕНЕННЫЙ ЗАПРОС ▼▼▼
                    # Теперь он также считает KDA для каждого игрока
                    # и сортирует СНАЧАЛА по KDA, потом по WR.
                    cur.execute('''
                        SELECT 
                            player_nickname, 
                            wins, 
                            losses,
                            (CAST(wins AS FLOAT) / (wins + losses)) * 100 AS player_wr,
                            (CASE 
                                WHEN total_deaths = 0 THEN (total_kills + total_assists)
                                ELSE (CAST(total_kills AS FLOAT) + total_assists) / total_deaths
                            END) AS player_kda
                        FROM player_heroes
                        WHERE hero_name = %s AND (wins + losses) >= %s
                        ORDER BY
                            player_kda DESC,
                            player_wr DESC,
                            wins DESC
                        LIMIT 1
                    ''', (hero, min_games))
                    # ▲▲▲ КОНЕЦ ИЗМЕНЕНИЙ ▲▲▲
                    
                    best_player = cur.fetchone()
                    
                    text += f"<b>{idx}. {hero}</b>\n"
                    text += f"    - <b>Игр:</b> {games} | <b>WR:</b> {wr:.1f}% | <b>KDA:</b> {kda:.2f}\n"
                    
                    # ▼▼▼ ИЗМЕНЕННЫЙ ВЫВОД ▼▼▼
                    if best_player:
                        # Теперь мы получаем 5 значений, включая p_kda
                        p_nick, p_w, p_l, p_wr, p_kda = best_player 
                        # Добавляем KDA в строку "Лучший игрок"
                        text += f"    - <b>Лучший игрок:</b> {p_nick} ({p_w}W-{p_l}L, <b>KDA: {p_kda:.2f}</b>, {p_wr:.1f}% WR)\n\n"
                    else:
                        text += f"    - <b>Лучший игрок:</b> (Мало данных)\n\n"
                    # ▲▲▲ КОНЕЦ ИЗМЕНЕНИЙ ▲▲▲
                        
                return text

    except Exception as e:
        print(f"Ошибка get_global_hero_stats_text: {e}")
        return "Ошибка выполнения запроса к БД."
    finally:
        put_db_conn(conn)

@bot.message_handler(commands=['statshero'])
def show_global_hero_stats(message):
    log_user_activity(message.from_user.id, message)
    # Ищем лучшего игрока с мин. 3 играми на герое
    text = get_global_hero_stats_text(min_games=2) 
    try:
        bot.reply_to(message, text)
    except Exception as e:
        print(f"Ошибка statshero: {e}")
# ▲▲▲ КОНЕЦ НОВЫХ ФУНКЦИЙ ▲▲▲


@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_undo_"))
def handle_undo_game_confirmation(call):
    if not is_admin(call.from_user.id):
        try: bot.answer_callback_query(call.id, "❌ Доступ запрещён!", show_alert=True)
        except Exception: pass
        return
    conn = get_db_conn()
    if not conn:
        bot.send_message(call.message.chat.id, "❌ Ошибка БД")
        return
    try:
        bot.answer_callback_query(call.id, "🔄 Начинаю откат...")
        game_id = int(call.data.replace("confirm_undo_", ""))
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM games WHERE id=%s", (game_id,))
                game_data = cur.fetchone()
                if not game_data:
                    bot.edit_message_text("❌ Ошибка: Эта игра уже не существует.", call.message.chat.id, call.message.message_id)
                    return
                game_columns = [desc[0] for desc in cur.description]
                game = dict(zip(game_columns, game_data))
                cur.execute("SELECT * FROM player_game_stats WHERE game_id=%s", (game_id,))
                stats_data = cur.fetchall()
                stats_columns = [desc[0] for desc in cur.description]
                player_stats = [dict(zip(stats_columns, row)) for row in stats_data]
                if not player_stats:
                    cur.execute("DELETE FROM games WHERE id = %s", (game_id,))
                    bot.edit_message_text(f"✅ Игра {game_id} (без статистики) была удалена.", call.message.chat.id, call.message.message_id)
                    return
                winners_team = game['result']
                for p_stat in player_stats:
                    nickname = p_stat['player_nickname']
                    hero = p_stat['hero']
                    kills = p_stat['kills']
                    deaths = p_stat['deaths']
                    assists = p_stat['assists']
                    position = p_stat['position']
                    team = p_stat['team']
                    is_winner = (team == winners_team)
                    win_change = -1 if is_winner else 0
                    loss_change = 0 if is_winner else -1
                    rating_change_undo = -RATING_CHANGE if is_winner else RATING_CHANGE
                    cur.execute("SELECT rating FROM players WHERE nickname=%s FOR UPDATE", (nickname,))
                    current_rating = cur.fetchone()[0]
                    new_rating = current_rating + rating_change_undo
                    if not is_winner:
                        if current_rating == 0:
                           original_loss_rating_change = max(0, current_rating - RATING_CHANGE) - current_rating
                           rating_change_undo = -original_loss_rating_change
                           new_rating = current_rating + rating_change_undo
                    new_rating = max(0, new_rating)
                    cur.execute(
                        '''UPDATE players SET 
                                wins = wins + %s, losses = losses + %s, rating = %s, 
                                total_kills = total_kills - %s, total_deaths = total_deaths - %s, 
                                total_assists = total_assists - %s
                           WHERE nickname=%s''',
                        (win_change, loss_change, new_rating, kills, deaths, assists, nickname)
                    )
                    cur.execute(
                        '''UPDATE player_heroes SET
                                wins = wins + %s, losses = losses + %s,
                                total_kills = total_kills - %s,
                                total_deaths = total_deaths - %s,
                                total_assists = total_assists - %s
                           WHERE player_nickname = %s AND hero_name = %s''',
                        (win_change, loss_change, kills, deaths, assists, nickname, hero)
                    )
                    if position > 0:
                        cur.execute(
                            '''UPDATE player_role_stats SET
                                    wins = wins + %s, losses = losses + %s
                               WHERE player_nickname = %s AND role_position = %s''',
                            (win_change, loss_change, nickname, position)
                        )
                cur.execute("DELETE FROM games WHERE id = %s", (game_id,))
        bot.edit_message_text(f"✅ <b>ОТКАТ УСПЕШЕН!</b>\n\n"
                                f"Игра <b>ID {game_id}</b> и вся связанная с ней статистика "
                                f"были полностью удалены из базы данных.", 
                                call.message.chat.id, call.message.message_id)
        print(f"✅✅✅ Транзакция ОТКАТА ИГРЫ {game_id} успешно завершена.")
        player_cache.invalidate() # <-- ОБНОВЛЯЕМ КЭШ
    except Exception as e:
        print(f"❌❌❌ ОШИБКА ОТКАТА ИГРЫ: {e}")
        import traceback
        traceback.print_exc()
        bot.send_message(call.message.chat.id, f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ОТКАТЕ:\n<code>{str(e)}</code>\n\n"
                                                "🚫 <b>Изменения отменены.</b> База данных в безопасности.")
    finally:
        put_db_conn(conn)
# ===== ОБРАБОТЧИКИ ДЛЯ 4 ОТСУТСТВУЮЩИх ФУНКЦИЙ =====

# 1. ИЗМЕНИТЬ РЕЙТИНГ
@bot.callback_query_handler(func=lambda call: call.data.startswith("select_for_set_rating_"))
def select_player_for_set_rating(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    nickname = call.data.replace("select_for_set_rating_", "")
    try:
        bot.answer_callback_query(call.id)
        bot.delete_message(chat_id, call.message.message_id)
    except Exception: pass
    
    user_state[user_id] = {"action": "waiting_set_rating_value", "nickname": nickname}
    bot.send_message(chat_id, f"📝 Введите новый рейтинг для {nickname}:\nПример: 1500")

@bot.message_handler(func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_set_rating_value")
def handle_set_rating_value(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    if user_id not in user_state: return
    
    state = user_state[user_id]
    nickname = state["nickname"]
    
    try:
        new_rating = int(message.text.strip())
        if new_rating < 0:
            bot.send_message(chat_id, "❌ Рейтинг не может быть отрицательным!")
            return
    except ValueError:
        bot.send_message(chat_id, "❌ Введите число!")
        return
    
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE players SET rating=%s WHERE nickname=%s", (new_rating, nickname))
        bot.send_message(chat_id, f"✅ Рейтинг {nickname} изменён на {new_rating}!")
        player_cache.invalidate()
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)
        del user_state[user_id]

# 2. ДОБАВИТЬ MMR
@bot.callback_query_handler(func=lambda call: call.data.startswith("select_for_add_mmr_"))
def select_player_for_add_mmr(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    nickname = call.data.replace("select_for_add_mmr_", "")
    try:
        bot.answer_callback_query(call.id)
        bot.delete_message(chat_id, call.message.message_id)
    except Exception: pass
    
    user_state[user_id] = {"action": "waiting_add_mmr_value", "nickname": nickname}
    bot.send_message(chat_id, f"🎖️ Введите MMR для {nickname}:\nПример: 5000")

@bot.message_handler(func=lambda message: user_state.get(message.from_user.id, {}).get("action") == "waiting_add_mmr_value")
def handle_add_mmr_value(message):
    if not is_admin(message.from_user.id): return
    user_id = message.from_user.id
    chat_id = message.chat.id
    if user_id not in user_state: return
    
    state = user_state[user_id]
    nickname = state["nickname"]
    
    try:
        new_mmr = int(message.text.strip())
        if new_mmr < 0:
            bot.send_message(chat_id, "❌ MMR не может быть отрицательным!")
            return
    except ValueError:
        bot.send_message(chat_id, "❌ Введите число!")
        return
    
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE players SET mmr=%s WHERE nickname=%s", (new_mmr, nickname))
        bot.send_message(chat_id, f"✅ MMR {nickname} установлен на {new_mmr}!")
        player_cache.invalidate()
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)
        del user_state[user_id]

# 3. УСТАНОВИТЬ ПОЗИЦИИ
@bot.callback_query_handler(func=lambda call: call.data.startswith("select_for_set_positions_"))
def select_player_for_set_positions(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    nickname = call.data.replace("select_for_set_positions_", "")
    try:
        bot.answer_callback_query(call.id)
        bot.delete_message(chat_id, call.message.message_id)
    except Exception: pass
    
    user_state[user_id] = {"action": "waiting_select_positions", "nickname": nickname, "selected_positions": []}
    
    markup = types.InlineKeyboardMarkup()
    for pos_id, pos_name in POSITIONS.items():
        markup.add(types.InlineKeyboardButton(f"{pos_id}. {pos_name}", callback_data=f"toggle_position_{pos_id}"))
    markup.add(types.InlineKeyboardButton("✅ Готово", callback_data="confirm_positions"))
    # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
    
    bot.send_message(chat_id, f"🎯 Выберите предпочитаемые позиции для {nickname}:\n(Нажимайте для выбора)", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle_position_"))
def toggle_position(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in user_state: return
    
    state = user_state[user_id]
    pos_id = int(call.data.replace("toggle_position_", ""))
    
    if pos_id in state["selected_positions"]:
        state["selected_positions"].remove(pos_id)
    else:
        state["selected_positions"].append(pos_id)
    
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    markup = types.InlineKeyboardMarkup()
    for p_id, p_name in POSITIONS.items():
        prefix = "✅ " if p_id in state["selected_positions"] else ""
        markup.add(types.InlineKeyboardButton(f"{prefix}{p_id}. {p_name}", callback_data=f"toggle_position_{p_id}"))
    markup.add(types.InlineKeyboardButton("✅ Готово", callback_data="confirm_positions"))
    # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
    
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass

@bot.callback_query_handler(func=lambda call: call.data == "confirm_positions")
def confirm_positions(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    if user_id not in user_state: return
    
    state = user_state[user_id]
    nickname = state["nickname"]
    selected_positions = state["selected_positions"]
    
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    
    try:
        with conn:
            with conn.cursor() as cur:
                positions_json = json.dumps(sorted(selected_positions))
                cur.execute("UPDATE players SET positions=%s WHERE nickname=%s", (positions_json, nickname))
        
        pos_str = get_player_positions_str(selected_positions)
        bot.edit_message_text(f"✅ Предпочитаемые позиции {nickname} установлены:\n{pos_str}", chat_id, call.message.message_id)
        player_cache.invalidate()
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)
        del user_state[user_id]

# ▼▼▼ ИСПРАВЛЕНИЕ 3.5: УДАЛЕНИЕ СТАРОЙ ФУНКЦИИ ОТМЕНЫ ▼▼▼
# @bot.callback_query_handler(func=lambda call: call.data == "cancel_set_positions")
# ... (ФУНКЦИЯ УДАЛЕНА) ...
# ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.5 ▲▲▲


# 4. УДАЛИТЬ ИГРОКА
@bot.callback_query_handler(func=lambda call: call.data.startswith("select_for_delete_player_"))
def select_player_for_delete(call):
    if not is_admin(call.from_user.id): return
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    nickname = call.data.replace("select_for_delete_player_", "")
    
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{nickname}"))
    # ▼▼▼ ИСПРАВЛЕНИЕ 3.4: ЗАМЕНА КНОПКИ "ОТМЕНА" ▼▼▼
    markup.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back_to_admin_panel"))
    # ▲▲▲ КОНЕЦ ИСПРАВЛЕНИЯ 3.4 ▲▲▲
    
    try:
        bot.edit_message_text(f"⚠️ Вы уверены, что хотите удалить {nickname}?\n\nЭто удалит всю его статистику!", 
                              chat_id, call.message.message_id, reply_markup=markup)
    except Exception: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete_"))
def confirm_delete_player(call):
    if not is_admin(call.from_user.id): return
    chat_id = call.message.chat.id
    nickname = call.data.replace("confirm_delete_", "")
    
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    
    conn = get_db_conn()
    if not conn:
        bot.send_message(chat_id, "❌ Ошибка БД")
        return
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM players WHERE nickname=%s", (nickname,))
        
        bot.edit_message_text(f"✅ Игрок {nickname} удалён!", chat_id, call.message.message_id)
        player_cache.invalidate()
    except Exception as e:
        print(f"❌ Ошибка удаления: {e}")
        bot.send_message(chat_id, f"❌ Ошибка: {str(e)}")
    finally:
        put_db_conn(conn)

# ===== КОНЕЦ ОБРАБОТЧИКОВ =====
# ===== ГЛАВНЫЙ ЦИКЛ (НОВАЯ ВЕРСИЯ ДЛЯ RENDER/GUNICORN) =====

def run_bot_polling():
    """Запускает polling бота в бесконечном цикле в отдельном потоке."""
    print("🚀 [THREAD] ЗАПУСК TELEGRAM БОТА (polling)...")
    
    create_tables()
    
    while True:
        try:
            bot.polling(non_stop=True, timeout=20)
        except Exception as e:
            print(f"🔥🔥🔥 Ошибка polling: {e}")
            try:
                bot.stop_polling()
            except Exception as e2:
                print(f"🔥🔥🔥 Ошибка при остановке polling: {e2}")
            time.sleep(10)

if __name__ != "__main__":
    print("🌀 [MAIN] Запуск потока для bot.polling()...")
    t_bot = Thread(target=run_bot_polling)
    t_bot.daemon = True
    t_bot.start()

print("🌐 [MAIN] Flask-сервер (gunicorn) готов к запуску.")

if __name__ == "__main__":
    print("🔴 [LOCAL] Запускаем бота локально (НЕ GUNICORN)...")

    run_bot_polling()

