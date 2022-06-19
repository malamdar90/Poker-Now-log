
import numpy as np
import pandas as pd
from constants import *
from parse import *


def truncate_all(cur):
    print("Truncating existing tables...")
    cur.execute("SHOW TABLES")
    trunc = list(cur)
    for x in trunc:
        cur.execute("truncate poker." + x[0] + ";")


def icon_to_char(vec):
    vec = vec.replace("♥", "H")
    vec = vec.replace("♦", "D")
    vec = vec.replace("♣", "C")
    vec = vec.replace("♠", "S")
    vec = vec.replace("10", "T")
    return vec


def get_table_settings(cur):
    cur.execute("select max(table_id)+1 as next_table_id from tables;")
    table_id = list(cur)[0][0]
    if not table_id:
        table_id = 1
    cur.execute("select max(event_id)+1 as next_event_id from events;")
    first_event_id = list(cur)[0][0]
    if not first_event_id:
        first_event_id = 1
    cur.execute("select max(sh_id)+1 as next_sh_id from seat_hand;")
    first_sh_id = list(cur)[0][0]
    if not first_sh_id:
        first_sh_id = 1
    cur.execute("select max(fl_id)+1 as next_fl_id from floor_actions;")
    first_fl_id = list(cur)[0][0]
    if not first_fl_id:
        first_fl_id = 1

    players = set()
    cur.execute("SELECT player_id FROM poker.players;")
    existing_players = list(cur)
    for player in existing_players:
        players.add(player[0])

    return players, first_fl_id, first_sh_id, first_event_id, table_id


def read_datafile(filename, first_event_id):
    data = pd.read_csv(
        "raw_files/poker_now_log_"
        + filename
        + ".csv"
    )
    nrow = data.shape[0]
    data["event_num"] = np.arange(nrow, 0, -1)
    data["event_id"] = np.arange(
        nrow + first_event_id - 1, first_event_id - 1, -1)
    return data.sort_values("event_id")


def add_table(cur, table_id, table_info):
    filename, week, table_number = table_info
    sql = "INSERT INTO tables (table_id, week, number, filename) VALUES (%s, %s, %s, %s);"
    cur.execute(sql, (table_id, week, table_number, filename))


def parse_data(cur, data, table_id):
    sql = "INSERT INTO events (event_id, event_num, type, at, entry) VALUES (%s, %s, %s, %s, %s);"
    pat = {}
    hand = 0
    rnd = 0
    type_list = [np.nan] * data.shape[0]
    hand_list = [0] * data.shape[0]

    for j in range(data.shape[0]):
        rec = -1
        txt = data.entry.iloc[j]
        id1 = data.event_id.iloc[j]
        event_time = data["at"].iloc[j]

        for i, pattern in enumerate(patterns):
            a = parse(pattern, txt)

            if a is not None:
                prev = pat.get(i)

                if i in (0, 1):
                    hand += 1
                    rnd = 0

                if i == 13:
                    rnd = 1

                if i == 14:
                    rnd = 2

                if i == 15:
                    rnd = 3

                if i == 2:
                    ll = []
                    stacks = list(a)[0].split(" | ")
                    for u, user in enumerate(stacks):
                        b = list(parse('#{} "{} @ {}" ({})', user))
                        ll.append(b)
                    new_item = [id1, event_time, txt, ll, table_id, hand, rnd]
                else:
                    new_item = [id1, event_time, txt,
                                list(a), table_id, hand, rnd]

                if prev:
                    pat[i].append(new_item)
                else:
                    pat[i] = [new_item]

                rec = i
                break

        #######
        hand_list[j] = hand

        event_type = None
        if rec >= 0:
            event_type = rec
            #########
            type_list[j] = event_type
        else:
            prev = pat.get(-1)
            new_item = [id1, event_time, txt, table_id, hand, rnd]
            if prev:
                pat[-1].append(new_item)
            else:
                pat[-1] = [new_item]

        cur.execute(
            sql,
            (
                int(id1),
                int(data.event_num.iloc[j]),
                event_type,
                event_time[:-1],
                data.entry.iloc[j],
            ),
        )
    data['type'] = type_list
    data['hand'] = hand_list
    return pat, data


def add_hands(cur, data, table_id):
    sql = "INSERT INTO hands (table_id, hand_number, start_event) VALUES (%s, %s, %s);"
    hands = data.groupby(["hand"]).event_id.agg([np.min])
    for index, row in hands.iterrows():
        cur.execute(sql, (table_id, index, int(row[0])))


def extract_sh_ids(rows, first_sh_id):
    sh_ids = {}
    sh_id = first_sh_id
    for row in rows:
        for seat_hand in row[3]:
            sh_ids[(row[5], seat_hand[2])] = sh_id
            sh_id += 1
    return sh_ids


def extract_dealers(pat):
    dealers = {}
    for row in pat.get(0, []):
        dealers[row[5]] = row[3][2]
    for row in pat.get(1, []):
        dealers[row[5]] = None
    return dealers


def add_seat_hands(cur, rows, sh_ids, dealers, table_id, players):
    sql = (
        "INSERT INTO seat_hand (sh_id, table_id, hand_number, seat_number, player_id, ini_stack, is_dealer)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s);"
    )
    sql2 = "INSERT INTO players (player_id, names) VALUES (%s, %s);"

    for row in rows:
        ll = row[3]
        for seat_hand in ll:
            userid = seat_hand[2]
            is_dealer = 0
            if dealers[row[5]] == userid:
                is_dealer = 1

            sh_id = sh_ids[(row[5], seat_hand[2])]
            cur.execute(
                sql,
                (
                    sh_id,
                    table_id,
                    row[5],
                    int(seat_hand[0]),
                    seat_hand[2],
                    int(seat_hand[3]),
                    is_dealer,
                ),
            )

            if userid not in players:
                players.add(userid)
                cur.execute(sql2, (userid, seat_hand[1]))


def add_player_actions(cur, pat, sh_ids):
    sql = (
        "INSERT INTO player_actions (`sh_id`, `round`, `order`, `event_id`, `amount`, `type`,  `allin`)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s);"
    )
    target_list = []

    for k in range(3, 11):
        for row in pat.get(k, []):
            if k in range(3, 9):
                amount = row[3][2]
                action = action_round[k][0]
            elif k in range(9, 11):
                amount = 0
                action = action_round[k][0]
            else:
                amount = row[3][3]
                if row[3][2] == "small blind":
                    action = 6
                elif row[3][2] == "big blind":
                    action = 7
                else:
                    action = 8

            sh_id = sh_ids[(row[5], row[3][1])]

            target_list.append(
                [
                    sh_id,
                    row[0],
                    amount,
                    action,
                    action_round[k][1],
                    row[5],
                    row[3][1],
                    row[6],
                ]
            )

    player_actions = pd.DataFrame(
        target_list,
        columns=[
            "sh_id",
            "event_id",
            "amount",
            "type",
            "allin",
            "hand_number",
            "player_id",
            "round",
        ],
    ).sort_values(["hand_number", "player_id"])

    player_actions["order"] = (
        player_actions.groupby(
            ["hand_number", "player_id", "round"]).cumcount() + 1
    )

    for index, row in player_actions.iterrows():
        cur.execute(
            sql, (row[0], row[7], row[8], int(row[1]),
                  int(row[2]), int(row[3]), row[4])
        )


def create_floor(pat, first_fl_id):
    target_list = []
    for k in range(13, 16):
        for row in pat[k]:
            target_list.append([row[0], row[5], rounds[k], row[3]])

    floor = pd.DataFrame(
        target_list, columns=["event_id", "hand_number", "round", "cards"]
    )
    floor = floor.sort_values(["event_id"])
    floor["fl_id"] = np.arange(first_fl_id, first_fl_id + floor.shape[0])
    return floor


def add_floor_actions(cur, floor, table_id):
    sql = "INSERT INTO floor_actions (fl_id, table_id, hand_number, round, event_id) VALUES (%s, %s, %s, %s, %s);"
    sql2 = "INSERT INTO floor_cards (fl_id, card) VALUES (%s, %s);"

    for index, row in floor.iterrows():
        cards = row[3]
        fl_id = int(row[4])
        rnd = row[2]

        cur.execute(sql, (fl_id, table_id, int(
            row[1]), int(row[2]), int(row[0])))

        if rnd == 1:
            cur.execute(sql2, (fl_id, icon_to_char(cards[0])))
            cur.execute(sql2, (fl_id, icon_to_char(cards[1])))
            cur.execute(sql2, (fl_id, icon_to_char(cards[2])))

        elif rnd == 2:
            cur.execute(sql2, (fl_id, icon_to_char(cards[3])))

        elif rnd == 3:
            cur.execute(sql2, (fl_id, icon_to_char(cards[4])))


def add_won(cur, rows, sh_ids):
    sql = "INSERT INTO won (sh_id, amount, description, event_id) VALUES (%s, %s, %s, %s);"
    for row in rows:
        event_id = int(row[0])
        amount = int(row[3][2])
        sh_id = sh_ids[(row[5], row[3][1])]
        cur.execute(sql, (sh_id, amount, None, event_id))


def add_winning_cards(cur, rows, sh_ids):
    sql = "INSERT INTO won (sh_id, amount, description, event_id) VALUES (%s, %s, %s, %s);"
    sql2 = "INSERT INTO winning_cards (sh_id, card) VALUES (%s, %s);"
    target_list = []
    for row in rows:
        sh_id = sh_ids[(row[5], row[3][1])]
        target_list.append(
            [
                sh_id,
                int(row[3][2]),
                row[3][3],
                int(row[0]),
                row[3][4],
                row[3][5],
                row[3][6],
                row[3][7],
                row[3][8],
            ]
        )
    target_list = pd.DataFrame(
        target_list,
        columns=[
            "sh_id",
            "amount",
            "description",
            "event_id",
            "c1",
            "c2",
            "c3",
            "c4",
            "c5",
        ],
    )
    won = target_list.groupby(
        ["sh_id", "description", "c1", "c2", "c3", "c4", "c5"]
    ).agg({"amount": "sum", "event_id": "min"})

    for index, row in won.iterrows():
        cur.execute(sql, (index[0], int(row[0]), index[1], int(row[1])))

        for card in index[2:7]:
            cur.execute(sql2, (index[0], icon_to_char(card)[0:2]))


def add_uncalled_bets(cur, rows, sh_ids):
    sql = "INSERT INTO collected_uncalled_bet (sh_id, event_id, amount) VALUES (%s, %s, %s);"
    for row in rows:
        sh_id = sh_ids[(row[5], row[3][2])]
        cur.execute(sql, (sh_id, int(row[0]), row[3][0]))


def add_show_cards(cur, rows, sh_ids):
    sql = "INSERT INTO show_cards (sh_id, event_id) VALUES (%s, %s);"
    sql2 = "INSERT INTO player_cards (sh_id, card) VALUES (%s, %s);"
    for row in rows:
        sh_id = sh_ids[(row[5], row[3][1])]
        try:
            cur.execute(sql, (sh_id, int(row[0])))
            cur.execute(sql2, (sh_id, icon_to_char(row[3][2])))
            cur.execute(sql2, (sh_id, icon_to_char(row[3][3])))
        except Exception:
            pass
