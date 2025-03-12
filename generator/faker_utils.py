from faker import Faker

# Initialize Faker
fake = Faker()




# Generate a single record based on schema
def generate_record(fake, schema):
    record = {}
    for field in schema:
        field_name = field["name"]
        field_type = field["type"]

        if field_type == "string":
            if "name" in field_name:
                record[field_name] = fake.name()
            elif "email" in field_name:
                record[field_name] = fake.email()
            elif "phone" in field_name:
                record[field_name] = fake.phone_number()
            elif "address" in field_name:
                record[field_name] = fake.address()
            elif "city" in field_name:
                record[field_name] = fake.city()
            elif "state" in field_name:
                record[field_name] = fake.state()
            elif "zip" in field_name:
                record[field_name] = fake.zipcode()
            elif "country" in field_name:
                record[field_name] = fake.country()
            else:
                record[field_name] = fake.word()

        elif field_type == "integer":
            record[field_name] = fake.random_int(min=0, max=100)

        elif field_type == "float":
            record[field_name] = round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2)

        elif field_type == "date":
            record[field_name] = fake.date_of_birth(minimum_age=0, maximum_age=90).isoformat()

        elif field_type == "datetime":
            record[field_name] = fake.date_time_this_decade().isoformat()

        else:
            record[field_name] = None

    return record


