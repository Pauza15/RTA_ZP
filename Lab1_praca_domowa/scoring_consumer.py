from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='scoring-group-v2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def score_transaction(tx):
    score = 0

    # R1: duża kwota
    if tx['amount'] > 3000:
        score += 3

    # R2: elektronika + średnio duża kwota
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2

    # R3: noc (0–5)
    if tx.get('hour', 12) < 6:
        score += 2

    return score

print("START SCORING CONSUMER...")

for message in consumer:
    tx = message.value

    score = score_transaction(tx)

    print(f"TX: {tx['tx_id']} | {tx['amount']} PLN | SCORE: {score}")

    # ALERT jeśli podejrzane
    if score >= 3:
        alert = {
            'tx_id': tx['tx_id'],
            'amount': tx['amount'],
            'score': score,
            'category': tx['category']
        }

        producer.send('alerts', value=alert)

        print(f"   >>> ALERT WYSŁANY DO KAFKI <<<")