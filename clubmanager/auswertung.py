import datetime

from clubmanager.db_connector import DatabaseManager


def age(dob: datetime.date) -> int:
    today = datetime.date.today()
    this_year_birthday = datetime.date(today.year, dob.month, dob.day)
    if this_year_birthday < today:
        years = today.year - dob.year
    else:
        years = today.year - dob.year - 1
    return years


def string_to_date(dob_string: str) -> datetime.date:
    try:
        day, month, year = dob_string.split(".")
    except ValueError:
        print(f"Invalid format input: '{dob_string}' for function: '{string_to_date.__name__}'")
        return None
    return datetime.date(int(year), int(month), int(day))


def will_be_age(dob: datetime.date) -> int:
    today = datetime.date.today()
    return today.year - dob.year


class Auswertung:

    def __init__(self, database_connection: DatabaseManager):
        self.database = database_connection

    def get_average_age(self) -> int:
        if result := self.database.execute_query("select geburtstag from mitglieder"):
            summ = 0
            count = 0
            for p in result:
                if ret := string_to_date(p[0]):
                    summ += age(ret)
                    count += 1
                else:
                    print(f"Could not parse '{p[0]}' into a valid date.")
            return int((summ / count).__round__(2))

    def get_anniversaries(self) -> list:

        if result := self.database.execute_query("select vorname, nachname, geburtstag from mitglieder"):
            anniversary_list = []

            for person in result:
                day_of_birth = None
                if ret := string_to_date(person[2]):
                    day_of_birth = ret
                    if will_be_age(day_of_birth) in range(50, 150, 10) and person is not None:
                        anniversary_list.append({"name": person[0] + " " + person[1],
                                                 "birthday": day_of_birth,
                                                 "age": age(day_of_birth),
                                                 "fut_age": will_be_age(day_of_birth)})
                else:
                    print(f"Could not parse '{person[2]}' into a valid date.")
            return anniversary_list

    def print_anniversaries(self):
        if anniversaries := auswertung.get_anniversaries():
            first = True
            for an in anniversaries:
                if first:
                    first = False
                else:
                    print("------------------------")
                for i in an:
                    print(f"{i}: {an[i]}")

    def get_duty_by_member(self, id):
        result = database_manager.execute_query(f"select m.vorname, m.nachname, v.name, vt.startdatum , vtd.name "
                                                f"from mitglieder m, dienst_mitglied dm, veranstaltung_termin vt, "
                                                f"veranstaltung_termin_dienst vtd, veranstaltungen v "
                                                f"where m.mitglied_id='{id}' and "
                                                f"m.mitglied_id = dm.mitglied_id and "
                                                f"dm.dienst_id = vtd.vtd_id and "
                                                f"vtd.veranstaltung_termin_id = vt.veranstaltung_termin_id and "
                                                f"vt.veranstaltungs_id = v.veranstaltungs_id")
        for d in result:
            print(d)

    def get_duty_count(self, min_count=0):
        result = database_manager.execute_query(f"SELECT m.vorname, m.nachname, count(*) Anzahl "
                                                f"from mitglieder m, dienst_mitglied dm "
                                                f"where m.mitglied_id = dm.mitglied_id "
                                                f"group by m.vorname, m.nachname "
                                                f"order by Anzahl")
        for m in result:
            if m[2] >= min_count:
                print(m)

    def search_member(self, search_string: str) -> list:
        if result := database_manager.execute_query(
                f"SELECT * from mitglieder WHERE vorname like '%{search_string}%' or nachname like '%{search_string}%'"):
            for m in result:
                print(m)


if __name__ == "__main__":
    # Create new Database manager and establish connection
    database_manager = DatabaseManager()
    database_manager.connect()

    # database_manager.execute_insert("dienst_mitglied", mitglied_id="1000", dienst_id="1000", status="Test")

    # Create new auswertung object and give it the newly created connector object
    auswertung = Auswertung(database_manager)

    # Run methods from auswertung
    auswertung.search_member("Moritz")
    # auswertung.get_dienste_from_member('89')
    # auswertung.get_duty_count(7)
    # auswertung.print_anniversaries()
    # print(auswertung.get_average_age())
