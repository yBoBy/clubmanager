import csv
import datetime
import sqlite3 as lite

from db_connector import DatabaseManager


def parse_dienste_csv():
    database_manager = DatabaseManager()
    database_manager.connect()

    with open("Dienststatistiken.csv", newline="\r\n", encoding="UTF-8") as dienste:
        dienstreader = csv.reader(dienste, delimiter=';', dialect="excel")
        next(dienstreader)
        for row in dienstreader:
            nachname = row[0].split(" ")[0]
            vorname = " ".join(row[0].split(" ")[1:])
            datum = row[1]
            veranstaltung_name = row[2]
            dienst_name = row[3]
            schicht_zeit = row[4]
            teilnahmestatus = row[5]

            veranstaltungs_check(database_manager, veranstaltung_name, datum, dienst_name, schicht_zeit,
                                 teilnahmestatus, vorname, nachname)


def veranstaltungs_check(database_manager, v_name, v_datum, d_name, d_zeit, d_status, vorname, nachanme):
    mitglied_id = database_manager.execute_query(
        f"SELECT mitglied_id from mitglieder WHERE vorname='{vorname}' and nachname='{nachanme}'")[0][0]
    print(mitglied_id)
    if mitglied_id is None:
        print(vorname)
    veranstaltungs_id = None
    if result := database_manager.execute_query(f"SELECT veranstaltungs_id from veranstaltungen WHERE name='{v_name}'"):
        veranstaltungs_id = result[0][0]
    else:
        veranstaltungs_id = database_manager.execute_insert("veranstaltungen", name=v_name)

    startdatum = str(datetime.date(int(v_datum), 1, 1))
    veranstaltungs_termin_id = None
    if result := database_manager.execute_query(f"SELECT veranstaltung_termin_id from veranstaltung_termin WHERE "
                                                f"veranstaltungs_id='{veranstaltungs_id}' and "
                                                f"startdatum='{startdatum}'"):
        veranstaltungs_termin_id = result[0][0]
    else:
        veranstaltungs_termin_id = database_manager.execute_insert("veranstaltung_termin",
                                                                   veranstaltungs_id=veranstaltungs_id,
                                                                   startdatum=startdatum,
                                                                   enddatum=startdatum)
    vtd_id = None
    if result := database_manager.execute_query(f"SELECT vtd_id from veranstaltung_termin_dienst WHERE "
                                                f"veranstaltung_termin_id='{veranstaltungs_termin_id}' and "
                                                f"name='{d_name}' and "
                                                f"start='{d_zeit}'"):
        vtd_id = result[0][0]
    else:
        vtd_id = database_manager.execute_insert("veranstaltung_termin_dienst",
                                                 name=d_name,
                                                 start=d_zeit,
                                                 ende=d_zeit,
                                                 veranstaltung_termin_id=veranstaltungs_termin_id)
    if result := database_manager.execute_query(f"SELECT mitglied_id, dienst_id from dienst_mitglied WHERE "
                                                f"dienst_id='{vtd_id}' and "
                                                f"mitglied_id='{mitglied_id}'"):
        m_id = result[0][0]
        d_id = result[0][1]
    else:
        ids = database_manager.execute_insert("dienst_mitglied",
                                              mitglied_id=mitglied_id,
                                              dienst_id=vtd_id,
                                              status=d_status)


def parse_member_csv():
    with open("Mitgliederliste.csv", newline="\r\n",
              encoding="UTF-8") as members:
        memreader = csv.reader(members, delimiter=';', dialect="excel")
        next(memreader)
        for row in memreader:
            vorname = row[0]
            nachname = row[1]
            strasse_und_nummer = row[2]
            wohnort_und_gemeinde = row[3]
            geburstag = row[4]
            telefonnummer = ""
            if len(row[7]) > 0:
                telefonnummer = row[7].replace(" ", "")
            handynummer = ""
            beitrittsdatum = row[8]
            email = ""
            if len(row[10]) > 0:
                email = row[10]
            iban_und_bank = row[11]

            wohnort_id = gemeinde_check(wohnort_und_gemeinde, memreader.line_num)
            bank_id, iban = bank_check(iban_und_bank, memreader.line_num)
            hausnummer = ""
            strasse = ""
            if len(strasse_und_nummer.split(" ")) >= 2:
                hausnummer = strasse_und_nummer.split(" ")[-1]
                strasse = strasse_und_nummer[0:len(strasse_und_nummer) - len(hausnummer)]
            else:
                print(f"Invalid Straße-Hausnummer format in line [{memreader.line_num}]: {strasse_und_nummer}")

            cur = sql_connection.cursor()
            cur.execute("insert into mitglieder (vorname, nachname, geburtstag, telefonnummer, handynummer, "
                        "beitrittsdatum, email, iban, bank_id, strasse, hausnummer, ort_id) "
                        "values (?, ?, ?, ?, ?, ? , ? ,? ,? ,?, ? ,?)",
                        (vorname, nachname, geburstag, telefonnummer, handynummer, beitrittsdatum, email, iban, bank_id,
                         strasse, hausnummer, wohnort_id))
            sql_connection.commit()


def bank_check(input_value: str, num: int) -> (str, str):
    """

    :param input_value:
    :param num:
    :return:
    """
    global sql_connection
    iban = input_value[0:28]
    iban = iban.replace(" ", "")
    if len(iban) != 22:
        print(f"Invalid IBAN in line [{num}]: {iban}")
    else:
        blz = iban[4:12]
        cur = sql_connection.cursor()
        cur.execute("select * from banken where blz=?", (blz,))
        query = cur.fetchone()
        if query is None:
            print(f"Unknown Bankleitzahl in line [{num}]: {blz}")
            return "0", iban
        else:
            return query[0], iban
    return "0", iban


#
def gemeinde_check(input_value: str, num: int) -> str:
    """

    :param input_value:
    :param num:
    :return: id of the location data_row
    """
    global sql_connection

    result = input_value.split(" ")
    if len(result) == 3:
        result[1] = result[1].strip()
        plz = result[0]
        gemeinde = result[1]
        ort = result[2]

        cur = sql_connection.cursor()
        cur.execute("""select * from orte where plz=? and ortsteil=?""", (plz, ort))
        query = cur.fetchone()
        if query is None:
            try:
                cur.execute('''insert into orte('gemeinde', 'plz', 'ortsteil') values (?, ?, ?)''',
                            (gemeinde, plz, ort))
                sql_connection.commit()
                print(f"Created new location with Gemeinde={gemeinde} - PLZ={plz} - Ort={ort}")
            except lite.IntegrityError:
                print(f"Error duplicate Ortsteil: {ort}")

        else:
            return query[0]
    else:
        print(f"Wrong Format in line [{num}]: {result}")


parse_dienste_csv()
