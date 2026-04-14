from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='filter-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchiwanie transakcji...\n")

for message in consumer:
    tx = message.value

    amount = tx.get('amount', 0)

    # zawsze pokazujemy transakcję
    print(f"TX: {tx['tx_id']} | {amount} PLN | {tx['category']} | {tx['store']}")

    # ALERT tylko dla dużych kwot
    if amount > 1000:
        print(f"   >>> ALERT: wysoka kwota ({amount} PLN) <<<")