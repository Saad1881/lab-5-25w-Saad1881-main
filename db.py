import sqlite3

def get_connection():
    """Returns a connection object for the application database, 
        with the row factory set to Row so that row data can be referenced using
        either index or column names"""
    connection = sqlite3.connect("data.sqlite")

    # Allow for indexing of rows using either integers or column names
    # See https://docs.python.org/3/library/sqlite3.html#row-objects
    connection.row_factory = sqlite3.Row  

    # Enforce referential entegrity
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    return connection

def get_user(username):
    """Gets the user with the given username as a dict containing 
        the keys 'username' and 'password_hash'"""
    # TODO: Complete this method as per the docstring above
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT username, password_hash FROM user WHERE username = ?", (username,))
    result = cursor.fetchone()

    if result:
        return dict(result)
    return None
    pass

def update_password(username, password_hash):
    """Updates the password hash for the given username"""
    # TODO: Complete this method as per the docstring above
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("UPDATE user SET password_hash = ? WHERE username = ?", (password_hash, username)) 
    connection.commit() 
    pass

def add_person(data):
    """Inserts a new person row based on the given data (must be a dict with keys corresponding to the
        column names of the person table).
        The person data may also include a 'phone_numbers' array that contains any number of 
        phone number dicts of the form {'number','label'}
        """
    try:
        with get_connection() as cnx:
            cursor = cnx.cursor()
            sql = """INSERT INTO person 
                (first_name, last_name, birthday, email,
                address_line1, address_line2, city, prov, country, postcode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(sql, [
                data['first_name'],
                data['last_name'],
                data['birthday'],
                data['email'],
                data['address_line1'],
                data['address_line2'],
                data['city'],
                data['prov'],
                data['country'],
                data['postcode']
            ])

            # TODO: Insert the person's phone numbers too, and make sure to do this in the context of a transaction
            person_id = cursor.lastrowid

            phone_data = [(person_id, phone['number'], phone['label']) for phone in data.get('phone_numbers', [])]

            if phone_data:
                phone_sql = """INSERT INTO phone (person_id, number, label) VALUES (?, ?, ?)"""
                cursor.executemany(phone_sql, phone_data)

            cnx.commit()

        # The transaction begins when the 'with get_connection()' block is entered.
        # If any step (inserting the person or the phone numbers) fails, the transaction will be rolled back,
        # meaning none of the inserts will be saved to the database. This ensures atomicity, i.e., either all 
        # the inserts happen, or none of them do.
    except Exception as e:
        print(f"Error occurred: {e}")
        cnx.rollback()

def delete_person(id):
    """Deletes the person with the given id from the person table
        id must be an id that exists in the person table"""
    with get_connection() as cnx:
        cursor = cnx.cursor()
        sql = """DELETE FROM person WHERE person_id = ?"""
        return cursor.execute(sql, [id])

PERSON_SORTABLE_FIELDS = ('person_id','first_name','last_name','birthday','email')
PERSON_SORTABLE_FIELD_HEADINGS = ('ID','First Name','Last Name','Birthday','Email')
def get_people_list(order_by):

    assert order_by in PERSON_SORTABLE_FIELDS, "The order_by argument must be one of: " + ", ".join(PERSON_SORTABLE_FIELDS)

    with get_connection() as cnx:
        cursor = cnx.cursor()
        # TODO: Update this query to include phone number information as per lab instructions
        sql = """SELECT p.person_id AS person_id, p.first_name, p.last_name, p.birthday, p.email,
                        p.address_line1, p.address_line2, p.city, p.prov, p.country, p.postcode,
                        ph.number AS phone_number, ph.label AS phone_label
                    FROM person p
                    LEFT JOIN phone ph ON p.person_id = ph.person_id"""

        if order_by:
            sql += " ORDER BY " + order_by

        results = cursor.execute(sql).fetchall()

        people = []

        people_dict = {}
        # TODO: Update this loop so that it correctly adds a phone number list to each person
        for r in results:
            person_id = r['person_id']
            if person_id not in people_dict:
                people_dict[person_id] = {
                    'person_id': person_id,
                    'first_name': r['first_name'],
                    'last_name': r['last_name'],
                    'birthday': r['birthday'],
                    'email': r['email'],
                    'address_line1': r['address_line1'],
                    'address_line2': r['address_line2'],
                    'city': r['city'],
                    'prov': r['prov'],
                    'country': r['country'],
                    'postcode': r['postcode'],
                    'phone_numbers': []
                }

            if r['phone_number']:
                people_dict[person_id]['phone_numbers'].append({
                    'number': r['phone_number'],
                    'label': r['phone_label']
                })

        return list(people_dict.values())

def get_person_ids():
    """Returns a list of the person ids that exist in the database"""
    with get_connection() as cnx:
        cursor = cnx.cursor()
        sql = """SELECT person_id FROM person"""
        return [ row[0] for row in cursor.execute(sql).fetchall() ]
