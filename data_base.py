import sqlite3
from openpyxl import Workbook
from random import randint

def create_connect():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    return (conn, cursor)


def insert_data(data):
    con, cur = create_connect()
    while True:
        try:
            tags = [i[0] for i in cur.execute("SELECT tag FROM data_parser WHERE 1").fetchall()]
            break
        except:
            print("Error SQL: 'insert_data'")
    if data[0] not in tags:
        while True:
            try:
                cur.execute("INSERT INTO data_parser VALUES (?,?,?,?,?,?,?)", data)
                break
            except:
                print("Error SQL: 'insert_data'")
    con.commit()
    con.close()

def update_data(data):
    con, cur = create_connect()
    for key, value in data.items():
        #while True:
        #    try:
                cur.execute(f"""UPDATE data_parser
                            SET count_questions_tag = count_questions_tag + {value[0]} ,
                                count_answer_tag = count_answer_tag + {value[1]} ,
                                count_upvotes_tag = count_upvotes_tag + {value[2]} ,
                                count_view_tag = count_view_tag + {value[3]} ,
                                count_questions_owner_reputation_300 = count_questions_owner_reputation_300 + {value[4]}
                            WHERE tag = '{key}'""")
        #        break
        #    except:
        #        print("Error SQL: 'update_data'")
    con.commit()
    con.close()


def new_data_base():
    con, cur = create_connect()
    cur.execute("""CREATE TABLE data_parser
                  (tag text, 
                   count_questions_tag integer, 
                   count_answer_tag integer,
                   count_upvotes_tag integer, 
                   count_view_tag integer,
                   count_questions_owner_reputation_300 integer,
                   related_tags text)
                   """)
    con.commit()
    con.close()


def get_all_data():
    con, cur = create_connect()
    result = cur.execute("SELECT * FROM data_parser WHERE 1").fetchall()
    wb = Workbook()
    wb.create_sheet(title = 'Парсинг', index = 0)
    sheet = wb['Парсинг']

    sheet.append(['Тег', 'Кол-во вопросов', 'Кол-во ответов', 'Кол-во Upvotes', 'Кол-во views', 'Кол-во вопросов с owner rep > 300', 'Related tags'])

    for item in result:
        try:
            sheet.append(item)
        except:
            pass

    name_file = f'stackoverflow_{randint(0, 99999999)}.xlsx'
    wb.save(name_file)
    return name_file



def load_tags():
    con, cur = create_connect()
    result = cur.execute("SELECT tag, related_tags FROM data_parser WHERE 1").fetchall()
    all_tag = [i[0] for i in result]
    con.close()
    return all_tag


get_all_data()