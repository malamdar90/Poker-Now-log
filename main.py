from pickle import TRUE
import mysql.connector
import timeit
from pokernow_funcs import *


tbl_names = [
    ("pgl9tffcZE70kmMCmY-9xubQ4", 7, 1),
    ("pglPCzosOpnDgGc0Rv51aRtHr", 7, 2),
    ("pglTcIuMQxnPteO5qfj1fpbW1", 7, 3),
    ("pglDtVATo3uHlGPsr-SvqPtf4", 7, 4),
    ("pglSeuNHrFGIuEkLSahu-46kU", 7, 5),
]

remove_prev_tables = True


def create_connection():
    return mysql.connector.connect(user="your_user", password="your_password", host="your_host", database="your_database")


def main():
    if(remove_prev_tables):
        connection = create_connection()
        cur = connection.cursor()
        truncate_all(cur)
        connection.close()

    for index_tbl, table_info in enumerate(tbl_names):
        connection = create_connection()
        cursor = connection.cursor()

        start = timeit.default_timer()
        filename, week, table_number = table_info

        players, first_fl_id, first_sh_id, first_event_id, table_id = get_table_settings(
            cursor)
        data = read_datafile(filename, first_event_id)
        add_table(cursor, table_id, table_info)
        pat, data = parse_data(cursor, data, table_id)

        add_hands(cursor, data, table_id)
        floor = create_floor(pat, first_fl_id)
        add_floor_actions(cursor, floor, table_id)
        sh_ids = extract_sh_ids(pat[2], first_sh_id)
        dealers = extract_dealers(pat)
        add_seat_hands(cursor, pat[2], sh_ids, dealers, table_id, players)
        add_player_actions(cursor, pat, sh_ids)
        add_won(cursor, pat[17], sh_ids)
        add_winning_cards(cursor, pat[16], sh_ids)
        add_uncalled_bets(cursor, pat[18], sh_ids)
        add_show_cards(cursor, pat[19], sh_ids)

        connection.commit()
        connection.close()
        stop = timeit.default_timer()
        print(
            "Reading file " + str(index_tbl + 1) + "/" +
            str(len(tbl_names)) + " took ",
            round(stop - start, 1),
            "secs.",
        )


main()
