import pymongo
import csv
import datetime

#Під'єднуємося до MongoDB і до файлу
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client.db_zno

with open('time.txt', 'w') as l_file:
    size = 1000
    file_names = ["Odata2019File.csv", "Odata2020File.csv"]
    years = [2019, 2020]


    for j in range(2):
        file_name, year = file_names[j], years[j]
        with open(file_name, "r", encoding="cp1251") as csv_file:
            start_time = datetime.datetime.now()
            l_file.write(str(start_time)+' Початок\n')
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            # кількість вставлених документів з поточної групи i кількість вставлених груп
            i = 0
            number = 0
            document_b = []

            inserted = db.inserted_docs.find_one({"year": year})
            if inserted == None:
                inserted = 0
            else:
                inserted = inserted["num_docs"]

            for row in csv_reader:
                if number * size + i < inserted:
                    i += 1
                    if i == size:
                        i = 0
                        number += 1
                    continue

                document = row
                document['year'] = year
                document_b.append(document)
                i += 1
                # При 1000 значень записуємо дані

                if i == size:
                    i = 0
                    number += 1
                    db.collection_zno_data.insert_many(document_b)
                    document_bundle = []
                    if number == 1:
                        db.inserted_docs.insert_one({"num_docs": size, "year": year})
                    else:
                        db.inserted_docs.update_one({
                            "year": year, "num_docs": (number - 1) * size},
                            {"$inc": {
                                "num_docs": size
                            }  })
            if i != 0 and document_b:
                db.inserted_docs.update_one({
                    "year": year, "num_docs": number * size},
                    {"$inc": {
                        "num_docs": i
                    }  })
                db.collection_zno_data.insert_many(document_b)

            end_time = datetime.datetime.now()
            l_file.write(str(end_time) +' Кінець\n')
            print('time:', end_time - start_time, ' Кінець\n')

result = db.collection_zno_data.aggregate([

    {"$match": {"physTestStatus": "Зараховано"}},

    {"$group": {
        "_id": {
            "year": "$year",
            "regname": "$REGNAME"
        },
        "max_score": {
            "$max": "$physBall100"
        }
    }},

    {"$sort": {"_id": 1} }
])

with open('result.csv', 'w', encoding="utf-8") as new_csv_file:
    csv_writer = csv.writer(new_csv_file)
    csv_writer.writerow(['Область', 'Рік', 'Макс. бал з фізики'])
    for document in result:
        year = document["_id"]["year"]
        regname = document["_id"]["regname"]
        max_score = document["max_score"]
        csv_writer.writerow([regname, year, max_score])
